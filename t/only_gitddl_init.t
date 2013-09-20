use strict;
use warnings;
use Test::More;
use Test::Git;
use GitDDL::Migrator;

use File::Spec;
use File::Path 'make_path';
use DBI;

eval q[use DBD::SQLite;];
if ($@) {
    plan skip_all => 'DBD::SQLite is required to run this test';
}

has_git;

my $repo = test_repository;#( temp => [ CLEANUP => 0 ]);

my $gd = GitDDL::Migrator->new(
    work_tree => $repo->work_tree,
    ddl_file  => File::Spec->catfile('sql', 'ddl.sql'),
    dsn       => ['dbi:SQLite:dbname=:memory:', '', ''],
);

my $first_sql = <<__SQL__;
CREATE TABLE first (
    id INTEGER NOT NULL,
    name VARCHAR(191)
);
__SQL__

make_path(File::Spec->catfile($repo->work_tree, 'sql'));

open my $fh, '>', File::Spec->catfile($repo->work_tree, 'sql', 'ddl.sql') or die $!;
print $fh $first_sql;
close $fh;

$repo->run('add', File::Spec->catfile('sql', 'ddl.sql'));
$repo->run('commit', '--author', 'Daisuke Murase <typester@cpan.org>',
                     '-m', 'initial commit');

ok !$gd->database_version;

$gd->deploy( only_gitddl_init => 1 );

ok $gd->check_version, 'check_version ok';

done_testing;
