requires 'perl', '5.008001';
requires 'GitDDL';
requires 'Mouse';
requires 'SQL::Translator';

on 'test' => sub {
    requires 'Test::More', '0.98';
};

