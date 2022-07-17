CREATE KEYSPACE test_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    AND durable_writes = true;

USE test_keyspace;
CREATE TABLE gpa_table(
    dep_code text,
    semester text,
    student_id bigint,
    student_name text,
    gpa float,
    reg_year int,
    PRIMARY KEY ( (dep_code, semester), student_id ));
TRUNCATE gpa_table;