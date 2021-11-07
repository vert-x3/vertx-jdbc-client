create table insert_table
(
    id    int not null,
    lname varchar(255),
    fname varchar(255),
    dob   date,
    cdate timestamp(6),
    CONSTRAINT insert_table_pk PRIMARY KEY (id)
);

insert into insert_table
values (1, 'doe', 'john', TO_DATE('2001-01-01','YYYY-MM-DD'), TO_TIMESTAMP('2021-11-07 13:30:00', 'YYYY-MM-DD HH24:MI:SS'));
