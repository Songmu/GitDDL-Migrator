package GitDDL::Migrator;
use 5.008005;
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

sub diff {
    my ($self, $version) = @_;

    if (!$version) {
        if ($self->check_version) {
            croak 'ddl_version == database_version, should no differences';
        }
    }

    my $dsn0 = $self->dsn->[0];
    my $db
        = $dsn0 =~ /:mysql:/ ? 'MySQL'
        : $dsn0 =~ /:Pg:/    ? 'PostgreSQL'
        :                      do { my ($d) = $dsn0 =~ /dbi:(.*?):/; $d };

    my $tmp_fh = File::Temp->new;
    $self->_dump_sql_for_specified_coomit($self->database_version, $tmp_fh->filename);

    my $source = SQL::Translator->new;
    $source->parser($db) or croak $source->error;
    $source->translate($tmp_fh->filename) or croak $source->error;

    my $target = SQL::Translator->new;
    $target->parser($db) or croak $target->error;

    if (!$version) {
        $target->translate(File::Spec->catfile($self->work_tree, $self->ddl_file))
            or croak $target->error;
    }
    else {
        my $tmp_fh = File::Temp->new;
        $self->_dump_sql_for_specified_coomit($version, $tmp_fh->filename);
        $target->translate($tmp_fh->filename) or croak $target->error;
    }

    my $diff = SQL::Translator::Diff->new({
        output_db     => $db,
        source_schema => $source->schema,
        target_schema => $target->schema,
    })->compute_differences->produce_diff_sql;

    # ignore first line
    $diff =~ s/.*?\n//;

    $diff
}

sub upgrade_database {
    my ($self, $version) = @_;

    $self->_do_sql($self->diff($version));
    $self->insert_version($version);
}

sub insert_version {
    my ($self, $version) = @_;

    $version ||= $self->ddl_version;

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

