package GitDDL::Migrator;
use 5.008001;
use strict;
use warnings;

our $VERSION = "0.01";

use Carp qw/croak/;
use SQL::Translator;
use SQL::Translator::Diff;
use Time::HiRes qw/gettimeofday/;
use Try::Tiny;

use Mouse;
extends 'GitDDL';

has _db => (
    is => 'ro',
    default => sub {
        my $self = shift;
        my $dsn0 = $self->dsn->[0];
        my $db
            = $dsn0 =~ /:mysql:/ ? 'MySQL'
            : $dsn0 =~ /:Pg:/    ? 'PostgreSQL'
            :                      do { my ($d) = $dsn0 =~ /dbi:(.*?):/; $d };
    },
);

has _real_translator => (
    is      => 'ro',
    lazy    => 1,
    default => sub {
        my $self = shift;
        my $translator = SQL::Translator->new(
            parser      => 'DBI',
            parser_args => +{ dbh => $self->_dbh },
        );
        $translator->translate;
        $translator->producer($self->_db);

        if ($self->_db eq 'MySQL') {
            # cut off AUTO_INCREMENT. see. http://bugs.mysql.com/bug.php?id=20786
            my $schema = $translator->schema;
            for my $table ($schema->get_tables) {
                my @options = $table->options;
                if (my ($idx) = grep { $options[$_]->{AUTO_INCREMENT} } 0..$#options) {
                    splice $table->options, $idx, 1;
                }
            }
        }
        $translator;
    },
);

no Mouse;

sub database_version {
    my ($self) = @_;

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    my ($version) =
        $self->_dbh->selectrow_array('SELECT version FROM ' . $self->version_table . ' ORDER BY upgraded_at DESC');

    if (defined $version) {
        return $version;
    }
    else {
        croak "Failed to get database version, please deploy first";
    }
}

sub deploy {
    my ($self) = @_;

    my $version = try {
        open my $fh, '>', \my $stderr;
        local *STDERR = $fh;
        $self->database_version;
        close $fh;
    };

    if ($version) {
        croak "database already deployed, use upgrade_database instead";
    }

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    $self->_do_sql($self->_slurp(File::Spec->catfile($self->work_tree, $self->ddl_file)));

    $self->_do_sql(<<"__SQL__");
CREATE TABLE @{[ $self->version_table ]} (
    version VARCHAR(40) NOT NULL,
    upgraded_at VARCHAR(20) NOT NULL UNIQUE
);
__SQL__

    $self->insert_version;
}

sub _new_translator {
    my $self = shift;

    my $translator = SQL::Translator->new;
    $translator->parser($self->_db) or croak $translator->error;

    $translator;
}

sub _new_translator_of_version {
    my ($self, $version) = @_;

    my $tmp_fh = File::Temp->new;
    $self->_dump_sql_for_specified_coomit($version, $tmp_fh->filename);

    my $translator = $self->_new_translator;
    $translator->translate($tmp_fh->filename) or croak $translator->error;

    $translator;
}

sub _diff {
    my ($self, $source, $target) = @_;

    my $diff = SQL::Translator::Diff->new({
        output_db     => $self->_db,
        source_schema => $source->schema,
        target_schema => $target->schema,
    })->compute_differences->produce_diff_sql;

    # ignore first line
    $diff =~ s/.*?\n//;

    $diff
}

sub diff {
    my ($self, %args) = @_;

    my $version = $args{version};
    my $reverse = $args{reverse};

    if (!$version) {
        if ($self->check_version) {
            croak 'ddl_version == database_version, should no differences';
        }
    }

    my $source = $self->_new_translator_of_version($self->database_version);

    my $target;
    if (!$version) {
        $target = $self->_new_translator;
        $target->translate(File::Spec->catfile($self->work_tree, $self->ddl_file))
            or croak $target->error;
    }
    else {
        $target = $self->_new_translator_of_version($version);
    }

    my ($from, $to) = !$reverse ? ($source, $target) : ($target, $source);
    $self->_diff($from, $to);
}

sub real_diff {
    my $self = shift;

    my $source = $self->_new_translator_of_version($self->database_version);
    my $real   = $self->_real_translator;

    my $diff = SQL::Translator::Diff->new({
        output_db     => $self->_db,
        source_schema => $source->schema,
        target_schema => $real->schema,
    })->compute_differences;

    my @tabls_to_create = @{ $diff->tables_to_create };
    @tabls_to_create = grep {$_->name ne $self->version_table} @tabls_to_create;
    $diff->tables_to_create(\@tabls_to_create);

    my $diff_str = $diff->produce_diff_sql;
    # ignore first line
    $diff_str =~ s/.*?\n//;

    $diff_str;
}

sub check_ddl_mismatch {
    my $self = shift;

    my $real_diff = $self->real_diff;
    croak "Mismatch between ddl version and real database is found. Diff is:\n $real_diff"
        unless $real_diff =~ /\A\s*-- No differences found;\s*\z/ms;
}

sub rollback_diff {
    my $self = shift;

    my $sth = $self->_dbh->prepare('SELECT version FROM ' . $self->version_table . ' ORDER BY upgraded_at DESC');
    $sth->execute;

    my ($current_version) = $sth->fetchrow_array;
    my ($prev_version)    = $sth->fetchrow_array;

    croak 'No rollback target is found'
        unless $prev_version;

    $self->diff(version => $prev_version);
}

sub upgrade_database {
    my ($self, %args) = @_;

    my $version = $args{version};
    my $sql     = $args{sql} || $self->diff(version => $version);

    $self->_do_sql($sql);
    $self->insert_version($version);
}

sub insert_version {
    my ($self, $version) = @_;

    $version ||= $self->ddl_version;
    unless (length($version) == 40) {
        $version = $self->_restore_full_hash($version);
    }

    # steal from DBIx::Schema::Versioned
    my @tm = gettimeofday();
    my @dt = gmtime ($tm[0]);
    my $upgraded_at = sprintf("v%04d%02d%02d_%02d%02d%02d.%03.0f",
        $dt[5] + 1900,
        $dt[4] + 1,
        $dt[3],
        $dt[2],
        $dt[1],
        $dt[0],
        int($tm[1] / 1000), # convert to millisecs
    );

    $self->_dbh->do(
        "INSERT INTO @{[ $self->version_table ]} (version, upgraded_at) VALUES (?, ?)", {}, $version, $upgraded_at
    ) or croak $self->_dbh->errstr;
}

sub _restore_full_hash {
    my ($self, $version) = @_;
    $self->_git->run('rev-parse', $version);
}

sub vacuum {
    ...
}

1;
__END__

=encoding utf-8

=head1 NAME

GitDDL::Migrator - It's new $module

=head1 SYNOPSIS

    use GitDDL::Migrator;

=head1 DESCRIPTION

GitDDL::Migrator is ...

=head1 LICENSE

Copyright (C) Masayuki Matsuki.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=head1 AUTHOR

Masayuki Matsuki E<lt>y.songmu@gmail.comE<gt>

=cut

