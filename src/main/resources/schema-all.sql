DROP TABLE people IF EXISTS;

CREATE TABLE people  (
    person_id BIGINT IDENTITY NOT NULL PRIMARY KEY,
    first_name VARCHAR(20),
    last_name VARCHAR(20)
);


CREATE TABLE transformed (
    person_id BIGINT IDENTITY NOT NULL PRIMARY key,
    name VARCHAR(100),
    date_created DATE
);

-- INSERT INTO people (first_name, last_name) VALUES (:name, :dateCreated);



insert into people VALUES (1, 'Longhai', 'Cui');
insert into people VALUES (2, 'Hao', 'Leng');
insert into people VALUES (3, 'Gulia', 'Vikram');

select first_name, last_name from people;


