CREATE
    TABLE
        char_size_test (
            SIZE CHAR (10)
        )
; CREATE
    TABLE
        varchar_size_test (
            SIZE VARCHAR (10)
        )
; WITH test_data AS (
    SELECT
            SUBSTRING (
                md5 (
                    random ()::text
                )
                ,1
                ,10
            )
        FROM
            generate_series (
                1
                ,1000000
            )
)
,cahr _data_insert AS (
    INSERT
        INTO
            char_size_test SELECT
                    *
                FROM
                    test_data
) INSERT
    INTO
        varchar_size_test SELECT
                *
            FROM
                test_date
; -- Get the table size in human readable form
 SELECT
        pg_size_pretty (
            pg_relation_size ('char_size_test')
        ) AS char_size_test
        ,pg_size_pretty (
            pg_relation_size ('varchar_size_test')
        ) AS varchar_size_test
; -- Delete the tables data
 TRUNCATE
    char_size_test
; TRUNCATE
    varchar_size_test
; -- Insert data with fixed length  
 WITH test_date AS (
    SELECT
            SUBSTRING (
                md5 (
                    random (
                    ) : : text
                )
                ,1
                ,(
                    random (
                    ) * 10
                ) : : INT
            )
        FROM
            generate_series (
                1
                ,1000000
            )
)
,cahr _data_insert AS (
    INSERT
        INTO
            char_size_test SELECT
                    *
                FROM
                    test_date
) INSERT
    INTO
        varchar_size_test SELECT
                *
            FROM
                test_date
; SELECT
        pg_size_pretty (
            pg_relation_size ('char_size_test')
        ) AS char_size_test
        ,pg_size_pretty (
            pg_relation_size ('varchar_size_test')
        ) AS varchar_size_test
; -- Create tables



CREATE
    TABLE
        emulate_varchar (
            test VARCHAR (4)
        )
; --semantically equivalent to
 CREATE
    TABLE
        emulate_varchar (
            test TEXT
            ,CONSTRAINT test_length CHECK (
                length (test) <= 4
            )
        )
;

-- One can summarize the conversion between the timestamp with and without the time zone, as follows:

-- The expression timestamp without time zone AT TIME ZONE x is interpreted as follows: the timestamp will be converted from the time zone x to the session time zone.
-- The expression timestamp with time zone AT TIME ZONE x converts a timestamptz into a timestamp at the specified time zone x. The final result type is timestamp.

SELECT now(), now()::timestamp, now() AT TIME ZONE 'CST', now()::timestamp AT TIME ZONE 'CST';
--               now              |            now             |          timezone          |           timezone            
-- -------------------------------+----------------------------+----------------------------+-------------------------------
--  2017-08-12 16:38:11.854128-07 | 2017-08-12 16:38:11.854128 | 2017-08-12 17:38:11.854128 | 2017-08-12 15:38:11.854128-07
-- (1 row)


SELECT '2014-09-01 23:30:00.000000+00'::timestamptz -'2014-09-01 22:00:00.000000+00'::timestamptz = Interval '1 hour, 30 minutes';
--  ?column?
-- ----------
--  t
-- (1 row)
SELECT '10-11-2014'::date -'10-10-2014'::date = 1
--  ?column?
-- ----------
--  t

select date_trunc('day',  now());

--        date_trunc       
-- ------------------------
--  2017-08-12 00:00:00-07
-- (1 row)

-- select pg_typeof(date_trunc('day',  now()));
--         pg_typeof         
-- --------------------------
--  timestamp with time zone
-- (1 row)

-- In PostgreSQL, the view is internally modeled as a table with a _RETURN rule. So, the following two pieces of code are equivalent:

CREATE VIEW test AS SELECT 1 AS one;
CREATE TABLE test (one INTEGER);
CREATE RULE "_RETURN" AS ON SELECT TO test DO INSTEAD
    SELECT 1;

-- The preceding example is for the purpose of explanation only, it is not recommended to play with the
-- PostgreSQL catalogue, including the reserved rules, manually. Moreover, note that a table can be
-- converted to a view but not vice versa.

-- When one creates views, the created tables are used to maintain the dependency between the
-- created views. So when executing the following query:

SELECT * FROM test;
-- We actually execute the following:

SELECT * FROM(SELECT 1) AS test;
-- To understand views dependency, let us build a view using another view, as follows:

--date_trunc function is similar to trunc function for numbers,
CREATE VIEW day_only AS
SELECT date_trunc('day', now()) AS day;
CREATE VIEW month_only AS
SELECT date_trunc('month', day_only.day)AS month FROM day_only;

-- The preceding views, month_only and day_only, are truncating the time to day and month respectively. The month_only view depends on the day_only view. In order to drop the day_only view, one can use one of the following options:

-- First drop the month_only view followed by the day_only view:

car_portal=# DROP VIEW day_only;
-- ERROR:  cannot drop view day_only because other objects depend on it
-- DETAIL:  view month_only depends on view day_only
-- HINT:  Use DROP ... CASCADE to drop the dependent objects too.
car_portal=# DROP VIEW month_only;
DROP VIEW
car_portal=# DROP VIEW day_only;
DROP VIEW
-- Use the CASCADE option when dropping the view:
-- car_portal=# DROP VIEW day_only CASCADE;
-- NOTICE:  drop cascades to view month_only
-- DROP VIEW

car_portal=# DROP VIEW day_only CASCADE;
-- NOTICE:  drop cascades to view month_only
-- DROP VIEW

-- When replacing the view definition using the REPLACE keyword, the column list should be identical before and after the replacement, including the column type, name, and order. The following example shows what happens when trying to change the view column order:

car_portal=# CREATE OR REPLACE VIEW account_information AS SELECT ....

select regexp_split_to_table('a=1&b=2&c=3', '&') as key;
--  key 
-- -----
--  a=1
--  b=2
--  c=3
-- (3 rows)

select regexp_split_to_array('hello world', E'\\s+');
--  regexp_split_to_array 
-- -----------------------
--  {hello,world}
-- (1 row)

-- Time: 28.554 ms
select regexp_split_to_table('hello world', E'\\s+');
--  regexp_split_to_table 
-- -----------------------
--  hello
--  world
-- (2 rows)

-- EXCEPT INTERSECT in multi-column output

----

-- On successful completion, an INSERT command returns a command tag of the form

-- INSERT oid count

-- The count is the number of rows inserted. If count is exactly one, and the target table has OIDs,
-- then oid is the OID assigned to the inserted row. Otherwise oid is zero.

-- If the INSERT command contains a RETURNING clause, the result will be similar to that of a SELECT
-- statement containing the columns and values defined in the RETURNING list, computed over the
-- row(s) inserted by the command.


select CURRENT_DATE;
    date    
------------
 2017-08-12
(1 row)

select CURRENT_DATE;
    date    
------------
 2017-08-12
(1 row)

-- 

WITH test_account AS (
    INSERT
        INTO
            account
        VALUES (
            1000
            ,'test_first_name'
            ,'test_last_name'
            ,'test@email.com'
            ,'password'
        )
        ,(
            2000
            ,'test_first_name2'
            ,'test_last_name2'
            ,'test2@email.com'
            ,'password2'
        ) RETURNING account_id
)
,car AS (
    SELECT
            i AS car_model
        FROM
            (
            VALUES ('brand=BMW')
            ,('brand=WV')
            ) AS foo (i)
)
,manufacturing_date AS (
    SELECT
            'year=' || i AS DATE
        FROM
            generate_series (
                2015
                ,2014
                ,- 1
            ) AS foo (i)
) INSERT
    INTO
        account_history (
            account_id
            ,search_key
            ,search_date
        ) SELECT
                account_id
                ,car.car_model || '&' || manufacturing_date.date
                ,CURRENT_DATE
            FROM
                test_account
                ,car
                ,manufacturing_date
;

-- Index on expressions

CREATE index ON account(lower(first_name));
SELECT * FROM account WHERE lower(first_name) = 'foo';

-- In the PostgreSQL renaming conventions, the suffixes for unique and normal indexes are key and idx respectively.

-- One can also use the unique and partial indexes together. For example, let us assume that we have
-- a table called employee, where each employee must have a supervisor except for the company
-- head. We can model this as a self-referencing table, as follows:

CREATE TABLE employee (employee_id INT PRIMARY KEY, supervisor_id INT);
ALTER TABLE employee ADD CONSTRAINT supervisor_id_fkey FOREIGN KEY (supervisor_id) REFERENCES employee(employee_id);

-- To guarantee that only one row is assigned to a supervisor, we can add the following unique
-- index:

CREATE UNIQUE INDEX ON employee ((1)) WHERE supervisor_id IS NULL;

-- The unique index on the constant expression (1) will allow only one row with a null value. With
-- the first insert of a null value, a unique index will be built using the value 1. A second
-- attempt to add a row with a null value will cause an error, because the value 1 is already
-- indexed:

-- Composite index: Rule of thumb: index for equality first — then for ranges.
-- If you already maintain an index on (a,b), then it doesn't make sense to create another index on
-- just (a) - unless it is substantially smaller. The same is not true for (b,a) vs. (a)

SELECT * FROM pg_stat_all_indexes WHERE schemaname = 'car_portal_app' limit 1;

SELECT pg_size_pretty(pg_indexes_size ('car_portal_app.account_pkey'))

--        funcname        |                                       description
-- ------------------------+------------------------------------------------------------------------------------------
--  pg_column_size         | bytes required to store the value, perhaps with compression
--  pg_database_size       | total disk space usage for the specified database
--  pg_indexes_size        | disk space usage for all indexes attached to the specified table
--  pg_relation_size       | disk space usage for the main fork of the specified table or index
--  pg_table_size          | disk space usage for the specified table, 
--                           including TOAST, free space and visibility map
--  pg_tablespace_size     | total disk space usage for the specified tablespace
--  pg_total_relation_size | total disk space usage for the specified table and associated indexes

select pg_column_size(1::bigint), pg_column_size('a'::text), pg_column_size('abc'::text);
--  pg_column_size | pg_column_size | pg_column_size 
-- ----------------+----------------+----------------
--               8 |              5 |              7
-- (1 row)

--- create a test table
car_portal=# CREATE TABLE a (id int);
-- CREATE TABLE
car_portal=# CREATE INDEX a_index ON a(id);
-- CREATE INDEX
--- Piece of code1: cause locks
car_portal=# REINDEX INDEX a_index;
-- REINDEX
--- Piece of code 2: No locks
car_portal=# CREATE INDEX concurrently a_index_1 ON a(id);
-- CREATE INDEX
car_portal=# DROP index a_index;
-- DROP INDEX
car_portal=# ALTER INDEX a_index_1 RENAME TO a_index;
-- ALTER INDEX

----

-- It's called pg_typeof() although it's not exactly what you want

select pg_typeof(1), ROW(1, 'abc');
-- pg_typeof | row    
-- ----------+--------
-- integer   | (1,abc)

-- You can't however use pg_typeof(select * from t1), not even with a limit 1 because the function
-- requires a single expression as its input, not multiple columns. You could however do something
-- like: pg_typeof((select some_column from t1))

-- ROW is row constructor https://stackoverflow.com/questions/32164926/what-is-a-row-constructor-used-for

CREATE OR REPLACE FUNCTION is_updatable_view (text) RETURNS BOOLEAN AS
$$
  SELECT is_insertable_into='YES' FROM information_schema.tables WHERE table_type = 'VIEW' AND table_name = $1
$$
LANGUAGE SQL;

CREATE OR REPLACE FUNCTION benefit_ids(begin_date date, end_date date, employer int)
RETURNS TABLE(count numeric, min_date date, max_date date) AS
$$
SELECT sum(count), min(min_date), max(max_date) from (select count(1) count, min(service_begin_date) min_date, max(service_begin_date) max_date from claim where benefit_id is null and service_begin_date >= $1 and service_begin_date <= $2 and employer_id=$3 union all select count(1), min(fill_date) min_date, max(fill_date) max_Date from rx_claim where benefit_id is null and fill_date >= $1 and fill_date <= $2 and employer_id=$3) c; 
$$
LANGUAGE 'sql' STABLE;

-- PL/pgSQL

CREATE OR REPLACE FUNCTION fact(fact INT) RETURNS INT AS
$$
DECLARE
count INT = 1;
result INT = 1;
BEGIN
  FOR count IN 1..fact LOOP
    result = result* count;
  END LOOP;
  RETURN result;
END;
$$
LANGUAGE plpgsql;

----

car_portal=# CREATE user select_only;
CREATE ROLE
car_portal=# DO $$DECLARE r record;
BEGIN
    FOR r IN SELECT table_schema, table_name FROM information_schema.tables
             WHERE  table_schema = 'car_portal_app'
    LOOP
        EXECUTE 'GRANT SELECT ON ' || quote_ident(r.table_schema) || '.' || quote_ident(r.table_name) || ' TO select_only';
    END LOOP;
END$$;
DO
car_portal=# \z account
                                          Access privileges
     Schema     |  Name   | Type  |           Access privileges           | Column access privileges
----------------+---------+-------+---------------------------------------+--------------------------
 car_portal_app | account | table | car_portal_app=arwdDxt/car_portal_app+|
                |         |       | select_only=r/car_portal_app          |
(1 row)

----

CREATE DOMAIN text_without_space_and_null AS TEXT NOT NULL CHECK (value !~ '\s');

-- Another good use case for creating domains is to create distinct identifiers across several
-- tables, since some people tend to use numbers instead of names to retrieve information.

-- One can do that by creating a sequence and wrapping it with a domain:

CREATE SEQUENCE global_id_seq;
CREATE DOMAIN global_id INT DEFAULT NEXTVAL('global_id_seq') NOT NULL;

----

CREATE TYPE seller_information AS (seller_id INT, seller_name TEXT, number_of_advertisements BIGINT, total_rank float);

-- Then we can use the newly created data type as the return type of the function, as follows:

CREATE OR REPLACE FUNCTION seller_information (account_id INT ) RETURNS seller_information AS
$$
  SELECT
    seller_id,
    first_name || last_name as seller_name,
    count(*),
    sum(rank)::float/count(*)
  FROM
    account INNER JOIN
    seller_account ON (account.account_id = seller_account.account_id) LEFT JOIN
    advertisement ON (advertisement.seller_account_id = seller_account.seller_account_id)LEFT JOIN
    advertisement_rating ON (advertisement.advertisement_id = advertisement_rating.advertisement_id)
  WHERE
    account.account_id = $1
  GROUP BY
    seller_id,
    first_name,
    last_name
$$
LANGUAGE SQL;

----

-- Another approach to model the rank is to use the enum data types, as follows:

CREATE TYPE rank AS ENUM ('poor', 'fair', 'good', 'very good', 'excellent');

-- The psql \dT meta command is used to describe the enum data type. One could also use the function enum_range, as follows:

car_portal=# SELECT enum_range(null::rank);
--                enum_range
-- ----------------------------------------
--  {poor,fair,good,"very good",excellent}
-- (1 row)

CREATE TABLE rank_type_test (
  id SERIAL PRIMARY KEY,
  rank rank
);
INSERT into rank_type_test(rank) VALUES ('poor') , ('fair'), ('very good') ,( 'excellent'), ('good'), ('poor') ;

SELECT * FROM rank_type_test ORDER BY rank ASC;
--  id |   rank
-- ----+-----------
--  17 | poor
--  22 | poor
--  18 | fair
--  21 | good
--  19 | very good
--  20 | excellent
-- (6 rows)

----
-- Rule system

CREATE TABLE car_log (LIKE car);

ALTER TABLE car_log
ADD COLUMN car_log_action varchar (1) NOT NULL,
ADD COLUMN car_log_time TIMESTAMP WITH TIME ZONE NOT NULL;
CREATE RULE car_log AS
    ON INSERT TO car
    DO ALSO
        INSERT INTO car_log (car_id, number_of_owners, regestration_number, number_of_doors, car_log_action, car_log_time)
        VALUES (new.car_id, new.number_of_owners, new.regestration_number, new.number_of_doors, 'I', now());

-- ALTER sequence car_car_id_seq RESTART;  Resetting sequence

-- A common case of using rules is to ignore the CRUD operation on the table in order to protect the table against data changes. This scenario can be used to protect lookup tables or to ignore the CRUD operations, for example. In the car web portal database, let us assume that we would like to disable the logging of the user's search. A very simple technique to do so without changing the client codes is as follows:

CREATE RULE account_search_log_insert AS
    ON INSERT TO account_search_history
    DO INSTEAD NOTHING;

----

CREATE OR REPLACE FUNCTION car_log_trg () RETURNS TRIGGER AS
$$
BEGIN
  IF TG_OP = 'INSERT' THEN
    INSERT INTO car_log SELECT NEW.*, 'I', NOW();
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO car_log SELECT NEW.*, 'U', NOW();
  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO car_log SELECT OLD.*, 'D', NOW();
  END IF;
  RETURN NULL; --ignored since this is after trigger
END;
$$
LANGUAGE plpgsql;

-- To create the trigger, one needs to execute the following statement:

CREATE TRIGGER car_log AFTER INSERT OR UPDATE OR DELETE ON car FOR EACH ROW EXECUTE PROCEDURE car_log_trg ();

----

ALTER USER pkeni WITH SUPERUSER;

----

SET search_path to car_portal_app;
CREATE extension hstore;
CREATE TABLE car_portal_app.log
(
  schema_name text NOT NULL,
  table_name text NOT NULL,
  old_row hstore,
  new_row hstore,
  action TEXT check (action IN ('I','U','D')) NOT NULL,
  created_by text NOT NULL,
  created_on timestamp without time zone NOT NULL
);

-- The second step is to define the trigger function, as follows:

CREATE OR REPLACE FUNCTION car_portal_app.log_audit() RETURNS trigger  AS $$
DECLARE
  log_row log;
  excluded_columns text[] = NULL;
BEGIN
  log_row = ROW (
    TG_TABLE_SCHEMA::text,                       
    TG_TABLE_NAME::text,                          
    NULL,
    NULL,
    NULL,
    current_user::TEXT,
    current_timestamp
    );
      
  IF TG_ARGV[0] IS NOT NULL THEN
    excluded_columns = TG_ARGV[0]::text[];
  END IF;

  IF (TG_OP = 'INSERT') THEN
    log_row.new_row = hstore(NEW.*) - excluded_columns;
    log_row.action ='I';
  ELSIF (TG_OP = 'UPDATE' AND (hstore(OLD.*) - excluded_columns!= hstore(NEW.*)-excluded_columns)) THEN
      log_row.old_row = hstor(OLD.*) - excluded_columns;
    log_row.new_row = hstore(NEW.* )- excluded_columns;
    log_row.action ='U';
  ELSIF (TG_OP = 'DELETE') THEN
    log_row.old_row = hstore (OLD.*) - excluded_columns;
    log_row.action ='D';
  ELSE
    RETURN NULL; -- update on excluded columns
  END IF;

  INSERT INTO log SELECT log_row.*;
     
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER car_log_trg  AFTER INSERT OR UPDATE OR DELETE ON car_portal_app.car FOR EACH ROW EXECUTE PROCEDURE log_audit('{number_of_doors}');

car_portal=# INSERT INTO car_portal_app.car (car_id, number_of_owners, registration_number, number_of_doors) VALUES (DEFAULT, 1, '2015abcde', 5);
INSERT 0 1

car_portal=# TABLE car_portal_app.log;
-- -[ RECORD 1 ]----------------------------------------------------------------------------
-- schema_name | car_portal_app
-- table_name  | car
-- old_row     |
-- new_row     | "car_id"=>"10", "number_of_owners"=>"1", "registration_number"=>"2015abcde"
-- action      | I
-- created_by  | postgres
-- created_on  | 2015-02-14 11:56:47.483

----

CREATE OR REPLACE VIEW seller_account_info AS
SELECT
  account.account_id,
  first_name,
  last_name,
  email,
  password,
  seller_account_id,
  total_rank,
  number_of_advertisement,
  street_name,
  street_number,
  zip_code ,
  city
FROM
  account INNER JOIN
  seller_account ON (account.account_id = seller_account.account_id);

car_portal=# SELECT is_insertable_into FROM information_schema.tables WHERE table_name = 'seller_account_info';
-- -[ RECORD 1 ]------+---
-- is_insertable_into | NO

----

CREATE OR REPLACE FUNCTION seller_account_info_update () RETURNS TRIGGER AS
$$
DECLARE
  acc_id INT;
  seller_acc_id INT;
BEGIN
  IF (TG_OP = 'INSERT') THEN
    WITH
    inserted_account AS (
      INSERT INTO car_portal_app.account (account_id, first_name, last_name, password, email)
      VALUES (DEFAULT, NEW.first_name, NEW.last_name, NEW.password, NEW.email) RETURNING account_id
      ),
    inserted_seller_account AS (INSERT INTO car_portal_app.seller_account(seller_account_id, account_id, total_rank, number_of_advertisement, street_name, street_number, zip_code, city)
                               SELECT nextval('car_portal_app.seller_account_seller_account_id_seq'::regclass), account_id, NEW.total_rank, NEW.number_of_advertisement, NEW.street_name,
                                      NEW.street_number, NEW.zip_code, NEW.city FROM inserted_account RETURNING account_id, seller_account_id
      ) SELECT account_id, seller_account_id INTO acc_id, seller_acc_id FROM inserted_seller_account;
      NEW.account_id = acc_id;
      NEW.seller_account_id = seller_acc_id;
      RETURN NEW;
  ELSIF (TG_OP = 'UPDATE' AND OLD.account_id = NEW.account_id AND OLD.seller_account_id = NEW.seller_account_id) THEN
      UPDATE car_portal_app.account
      SET first_name = new.first_name, last_name = new.last_name, password= new.password, email = new.email
      WHERE account_id = new.account_id;
      UPDATE car_portal_app.seller_account
      SET total_rank = NEW.total_rank, number_of_advertisement = NEW.number_of_advertisement, street_name = NEW.street_name, street_number = NEW.street_number, zip_code = NEW.zip_code, city = NEW.city
      WHERE seller_account_id = NEW.seller_account_id;
      RETURN NEW;
      
    ELSIF (TG_OP = 'DELETE') THEN
      DELETE FROM car_portal_app.seller_account WHERE seller_account_id = OLD.seller_account_id;
      DELETE FROM car_portal_app.account WHERE account_id = OLD.account_id;
      RETURN OLD;
    ELSE
      RAISE EXCEPTION 'An error occurred for % operation', TG_OP;
      RETURN NULL;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- To run and test the trigger function, let us execute the following SQL statements:

CREATE  TRIGGER seller_account_info_trg INSTEAD OF INSERT OR UPDATE OR DELETE ON car_portal_app.seller_account_info FOR EACH ROW  EXECUTE PROCEDURE seller_account_info_update ();

car_portal=# INSERT INTO car_portal_app.seller_account_info (first_name, last_name, password, email, total_rank, number_of_advertisement, street_name, street_number, zip_code, city) VALUES
('test_first_name', 'test_last_name', 'test_password', 'test_email@test.com', NULL, 0, 'test_street_name', 'test_street_number', 'test_zip_code', 'test_city') RETURNING account_id, seller_account_id;
--  account_id | seller_account_id
-- ------------+-------------------
--          14 |                 8
-- (1 row)

-- INSERT 0 1
car_portal=# UPDATE car_portal_app.seller_account_info set email = 'teat@test.com' RETURNING seller_account_id;
--  seller_account_id
-- -------------------
--                  8
-- (1 row)

-- UPDATE 1
car_portal=# DELETE FROM car_portal_app.seller_account_info;
-- DELETE 1

----

planadvisor_10000=> \d pg_stat_all_indexes 
-- View "pg_catalog.pg_stat_all_indexes"
--     Column     |  Type  | Modifiers 
-- ---------------+--------+-----------
--  relid         | oid    | 
--  indexrelid    | oid    | 
--  schemaname    | name   | 
--  relname       | name   | 
--  indexrelname  | name   | 
--  idx_scan      | bigint | 
--  idx_tup_read  | bigint | 
--  idx_tup_fetch | bigint | 

planadvisor_10000=> select * from pg_stat_all_indexes where schemaname='master';
--   relid  | indexrelid | schemaname |                   relname                    |                          indexrelname                           | idx_scan | idx_tup_read | idx_tup_fetch 
-- ---------+------------+------------+----------------------------------------------+-----------------------------------------------------------------+----------+--------------+---------------
--   979815 |     979818 | master     | load_file_catalog                            | load_file_catalog_pkey                                          |      699 |          619 |           618
--   981013 |     986179 | master     | procedure_cost                               | ix_pc_cbsa_pcid                                                 |    12660 |        12229 |         12229

-------------------------------

-- SELECT 'a', 'aa''aa', E'aa\naa', $$aa'aa$$,  U&'\041C\0418\0420';
-- ?column? | ?column? | ?column? | ?column? | ?column?
-- ----------+----------+----------+----------+----------
--  a        | aa'aa    | aa      +| aa'aa    | МИР
--           |          | aa       |          |

-- SELECT $str1$SELECT $$dollar-quoted string$$;$str1$;
--  ?column?
-- ----------------------------------
--  SELECT $$dollar-quoted string$$;
-- Here the sequences $str1$ are the quotes,

SELECT B'01010101'::int, X'AB21'::int;
--  int4 | int4
-- ------+-------
--    85 | 43809

-----------------------------------------------------------------

-- SELECT [DISTINCT | ALL]
--   <expression>[[AS] <output_name>][, …]
-- [FROM <table>[, <table>… | <JOIN clause>…]
-- [WHERE <condition>]
-- [GROUP BY <expression>|<output_name>|<output_number>
--   [,…]]
-- [HAVING <condition>]
-- [ORDER BY  <expression>|<output_name>|<output_number>
--   [ASC | DESC] [NULLS FIRST | LAST] [,…]]
-- [OFFSET <expression>]
-- [LIMIT <expression>];

----

SELECT substring('this is a string constant',11,6);
--  substring
-- -----------
--  string

----

-- Scalar subqueries or scalar queries are queries that return exactly one column and one or zero records. They have no special syntax and their difference from non-scalar queries is nominal.

-- Consider the example:

 SELECT (SELECT 1) + (SELECT 2) AS three;
--  three
-- ----------
--         3

----

-- CASE WHEN <condition1> THEN <expression1> [WHEN <condition2> THEN <expression2> ...] [ELSE <expression n>] END

-- CASE <checked_expression> WHEN <value1> THEN <result1> [WHEN <value2> THEN <result2> ...] [ELSE <result_n>] END

-- Note that the Cartesian product is not the same as the result of the full outer join. Cartesian
-- product means all possible combinations of all records from the tables without any specific
-- matching rules. Full outer join returns pairs of records when they that match the join
-- conditions. And the records that do not have a pair in other table are returned separately.

----

-- Sometimes that might cause problems. For example, it is not possible to divide by zero, and in case one wanted to filter rows based on the result of division, this would not be correct:

SELECT * FROM t WHERE a<>0 AND b/a>0.5

-- Because it is not guaranteed that PostgreSQL will evaluate the first condition (a<>0) before the
-- second, and in case a = 0, this could cause an error. To be secure one should use a CASE
-- statement because the order of evaluation of CASE conditions is determined by the statement:

SELECT * FROM t
  WHERE CASE WHEN a=0 THEN false ELSE b/a>0.5 END

----

-- There is a BETWEEN construct that also relates to comparing:

x BETWEEN a AND b

-- The preceding code is equivalent to:

x>=a AND a<=b

-- The OVERLAPS operator checks to see if two ranges of dates overlap or not. An example would be:

SELECT 1 WHERE (date '2014-10-15', date '2014-10-31')  OVERLAPS (date '2014-10-25', date '2014-11-15');
--  ?column?
-- ----------
--         1

----

For example, to get car models whose names start with s and have exactly four characters, one can use the following query:

SELECT * FROM car_portal_app.car_model
  WHERE model ILIKE 's_ _ _';
 car_model_id |   marke    | model
--------------+------------+-------
           47 | KIA        | Seed
There are another two pattern matching operators: SIMILAR and ~. They check for pattern matching using regular expressions. The difference between them is that SIMILAR uses regular expression syntax defined in SQL standard, while ~ uses Portable Operating System Interface (POSIX) regular expressions.

In this example, one selects all car models whose names consist of exactly two words:

 SELECT * FROM car_portal_app.car_model
  WHERE model ~ '^\w+\W+\w+$';
 car_model_id |     marke     |    model
--------------+---------------+--------------
           21 | Citroen       | C4 Picasso
           33 | Ford          | C-Max
           34 | Ford          | S-Max
----

-- SQL allows the use of array types that mean several elements as a whole in one single value. This
-- can be used for enriching comparison conditions. For example, this checks if a is bigger than any
-- of x, y or z:

a > ANY (ARRAY[x, y, z])

-- The preceding code is equivalent to:

(a >  x OR a > y OR a > z)

-- This checks if a is bigger than either x, y, or z:

a >  ALL (ARRAY[x, y, z])

-- The preceding code is equivalent to:

(a > x AND a > y AND a > z )

----

-- The NOT IN construct with a subquery is sometimes very slow because the check for the
-- nonexistence of a value is more expensive than the opposite.

----

-- Aggregating functions are not allowed in the WHERE clause but it is possible to filter groups
-- that follow a certain condition. This is different from filtering in the WHERE clause because
-- WHERE filters input rows, and groups are calculated afterwards. The filtering of the groups is
-- done by the HAVING clause. That is very similar to the WHERE clause but only aggregating
-- functions are allowed there. The HAVING clause is specified after the GROUP BY clause. Suppose
-- one needs to know which car models have a number of cars greater than 5.

SELECT a.marke, a.model
  FROM car_portal_app.car_model a
    INNER JOIN car_portal_app.car b
      ON a.car_model_id=b.car_model_id
  GROUP BY a.marke, a.model
  HAVING count(*)>5;
--   marke  | model
-- ---------+-------
--  Opel    | Corsa
--  Peugeot | 208

----

-- It is not possible to refer to the internal elements of one subquery from inside of another. But
-- subqueries can refer to the elements of main query. For example, if it is necessary to count cars
-- for each car model, and select the top five most popular models, it can be done by using subquery
-- in this way:

SELECT marke, model,
  (
    SELECT count(*)
      FROM car_portal_app.car
      WHERE car_model_id = main.car_model_id
  )
  FROM car_portal_app.car_model main
  ORDER BY 3 DESC
  LIMIT 5;
--   marke  |  model   | count
-- ---------+----------+-------
--  Peugeot | 208      |     7
--  Opel    | Corsa    |     6
--  Jeep    | Wrangler |     5
--  Renault | Laguna   |     5
--  Peugeot | 407      |     5
-- (5 rows)

-- In the example the subquery in the Select-list refers to the table of the main query by its alias
-- main. The subquery is executed for each record received from the main table, using the value of
-- car_model_id in the WHERE condition.

----

-- Similarly, the following will never return any record, even if a has a NULL value:

SELECT * FROM t WHERE a = NULL

-- To check the expression for having NULL value, a special operator is used: IS NULL. The previous
-- query, if it is necessary to also select records when both a and b are NULL, should be changed
-- this way:

SELECT * FROM t WHERE a = b OR (a IS NULL AND b IS NULL)

----

SELECT true AND NULL, false AND NULL, true OR NULL,  false OR NULL, NOT NULL;
--  ?column? | ?column? | ?column? | ?column? | ?column?
-- ----------+----------+----------+----------+----------
--           | f        | t        |          |

----

-- NULLIF takes two arguments and returns NULL if they are equal. Otherwise it returns the value of the first argument. This is somehow the opposite of COALESCE:

NULLIF (a, b)

-- The preceding code is equivalent to:

CASE WHEN a = b THEN NULL ELSE a END

----

-- Also b-tree indexes which are most commonly used, do not index NULL values. Consider the query as follows:

SELECT * FROM t WHERE a IS NULL

-- The preceding code will not use an index if it is created on the column a.

----

-- By default, the INSERT statement returns the number of inserted records. But it is also possible
-- to return the inserted records themselves, or some of their fields. The output of the statement
-- is then similar to the output of the SELECT query. The RETURNING keyword, with the list of fields
-- to return, is used for this:

INSERT INTO car_portal_app.a
SELECT * FROM car_portal_app.b
RETURNING a_int;

--  a_int
-- -------
--      2
--      3
--      4
-- (3 rows)

----

-- Now, let’s assume we’d like to find the number of countries with a GDP higher than 40,000 for each year.

With standard SQL:2003, and now also with the newly released PostgreSQL 9.4, we can now take advantage of the new FILTER clause, which allows us to write the following query:

SELECT
  year,
  count(*) FILTER (WHERE gdp_per_capita >= 40000)
FROM
  countries
GROUP BY
  year

-- The above query will now yield:

-- year   count
-- ------------
-- 2012   4
-- 2011   5
-- 2010   4
-- 2009   4

-- And that’s not it! As always, you can use any aggregate function also as a window function simply by adding an OVER() clause to the end:

SELECT
  year,
  code,
  gdp_per_capita,
  count(*) 
    FILTER (WHERE gdp_per_capita >= 40000) 
    OVER   (PARTITION BY year)
FROM
  countries

-- The result would then look something like this:

-- year   code   gdp_per_capita   count
-- ------------------------------------
-- 2009   CA           40764.00       4
-- 2009   DE           40270.00       4
-- 2009   FR           40488.00       4
-- 2009   GB           35455.00       4

----

-- All aggregating functions can be used as window functions, with the exception of ordered-set and
-- hypothetical-set aggregates. User defined aggregating functions can also be used as window
-- functions. The presence of an OVER clause indicates that the function is a window function.

----

-- For example:

UPDATE car_portal_app.a SET a_int = b_int FROM car_portal_app.b;

-- This query is syntactically correct. However, it is known that in the table b there is more than one record. And which of them will be selected for every updated row is not determined, since no WHERE condition is specified. The same happens when the WHERE clause does not define the one-to-one matching rule:

UPDATE car_portal_app.a SET a_int = b_int
  FROM car_portal_app.b
  WHERE b_int>=a_int;

-- For each record of table a there is more than one record from table b where, b_int is more or
-- equal to a_int. That's why the result of this update is undefined. However, PostgreSQL will allow
-- this to be executed.

----

UPDATE car_portal_app.a SET a_int = 0 RETURNING *;
--  a_int | a_text
-- -------+--------
--      0 | one
--      0 | two
-- ...

----

-- Sometimes, it is better not to use CTE. For example, one could decide to preselect some columns
-- from the table thinking it would help the database to perform the query because of the reduced
-- amount of information to process. In that case, the query would be the following:

WITH car_subquery AS
(
  SELECT number_of_owners, manufacture_year,
      number_of_doors
    FROM car_portal_app.car
)
SELECT number_of_owners, number_of_doors
  FROM car_subquery
  WHERE manufacture_year = 2008;

-- But that has the opposite effect. PostgreSQL does not push the WHERE clause from the primary
-- query to the substatement. The database will retrieve all the records from the table, take three
-- columns from them, and store this temporary dataset in the memory. Then, the temporary data will
-- be queried using the predicate, manufacture_year = 2008. If there was an index on
-- manufacture_year, it would not be used because it is the temporary data being queried and not the
-- real table.

-- For that reason, the following query is executed five times faster than the preceding one even
-- though it seems almost the same:

SELECT number_of_owners, number_of_doors
  FROM car_portal_app.car
  WHERE manufacture_year = 2008;

----

WITH RECURSIVE subq (n, factorial) AS
(
  SELECT 1, 1
  UNION ALL
  SELECT n+1, factorial*(n+1) from subq WHERE n <5
)
SELECT * FROM subq;
-- n | factorial
-- ---+-----------
--  1 |         1
--  2 |         2
--  3 |         6
--  4 |        24
--  5 |       120
-- (5 rows)

----

CREATE TABLE family (parent text, child text);
INSERT INTO family VALUES (NULL, 'Alan'),
('Alan', 'Bert'), ('Alan', 'Bob'), ('Bert', 'Carl'), ('Bert', 'Carmen'), ('Bob', 'Cecil'), ('Cecil', 'Dave'), ('Cecil', 'Den');


WITH RECURSIVE genealogy (bloodline, parent, level) AS
(
  SELECT child, child, 0
    FROM family WHERE parent IS NULL
  UNION ALL
  SELECT g.bloodline || ' -> ' || f.child, f.child,
      g.level + 1
    FROM family f, genealogy g
    WHERE f.parent = g.parent
)
SELECT bloodline, level FROM genealogy;
--           bloodline           | level
-- ------------------------------+-------
--  Alan                         |     0
--  Alan -> Bert                 |     1
--  Alan -> Bob                  |     1
--  Alan -> Bert -> Carl         |     2
--  Alan -> Bert -> Carmen       |     2
--  Alan -> Bob -> Cecil         |     2
--  Alan -> Bob -> Cecil -> Dave |     3
--  Alan -> Bob -> Cecil -> Den  |     3
-- (8 rows)


-- There is a potential problem with those hierarchical queries. If the data contained cycles, the
-- recursive query would never stop if written in the same way as the preceding code. For example,
-- let's add another record into the table family:

INSERT INTO family VALUES ('Bert', 'Alan');

-- Now there is a cycle in the data: Alan is a child of his own child. To run the query, it is
-- necessary to somehow make the query stop. That can be done by checking if the child being
-- processed is already included in the bloodline, as follows:

WITH RECURSIVE genealogy
  (bloodline, parent, level, processed) AS
(
  SELECT child, child, 0, ARRAY[child]
    FROM family WHERE parent IS NULL
  UNION ALL
  SELECT g.bloodline || ' -> ' || f.child,
      f.child, g.level + 1, processed || f.child
    FROM family f, genealogy g
    WHERE f.parent = g.parent
      AND NOT f.child = ANY(processed)
)
SELECT bloodline, level FROM genealogy;
--           bloodline           | level
-- ------------------------------+-------
--  Alan                         |     0
--  Alan -> Bert                 |     1
--  Alan -> Bob                  |     1
--  Alan -> Bert -> Carl         |     2
--  Alan -> Bert -> Carmen       |     2
--  Alan -> Bob -> Cecil         |     2
--  Alan -> Bob -> Cecil -> Dave |     3
--  Alan -> Bob -> Cecil -> Den  |     3
-- (8 rows)

--

-- A simple example of dependency and interaction between CTEs would be as follows:

car_portal=# CREATE TABLE t (f int UNIQUE);
-- CREATE TABLE
car_portal=# INSERT INTO t VALUES (1);
-- INSERT 0 1
car_portal=# WITH del_query AS (DELETE FROM t)
INSERT INTO t VALUES (1);
-- ERROR:  duplicate key value violates unique constraint "t_f_key"

-- The last query failed because PostgreSQL tried to execute the main query before the CTE. But if
-- one creates a dependency that will make the CTE execute first, then the record will be deleted
-- and the new record will be inserted. In that case, the constraint will not be violated:

car_portal=#  WITH del_query AS
(DELETE FROM t RETURNING f)
INSERT INTO t SELECT 1
WHERE (SELECT count(*) FROM del_query) IS NOT NULL;
INSERT 0 1

-- In the preceding code snippet, the WHERE condition in the main query does not have any practical
-- meaning because the result of COUNT is never NULL. However, since the CTE is referenced in the
-- query, it is executed before the execution of the main query.

----

-- Window functions can access the values of other records of the same group (which is called a
-- partition in this case), although the number of records stays the same. When window functions are
-- used, no grouping is necessary, although possible.

-- Window functions are evaluated after grouping and aggregation. For that reason, the only places
-- in the SELECT query where the window functions are allowed are Select-List and the ORDER BY
-- clause.

----

-- portion starting with ROWS is the frame clause.  In the end, the frame clause is processed. It
-- means taking a subset from the whole partition to pass it to the window function. The subset is
-- called a window frame.

OVER (PARTITION BY a ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING)

SELECT
    count() OVER w,
    sum(b) OVER w,
    avg(b) OVER (w ORDER BY c ROWS BETWEEN 1 PRECEDING
      AND 1 FOLLOWING)
  FROM table1
  WINDOW w AS (PARTITION BY a)


-- The presence of an OVER clause indicates that the function is a window function.

-- The first window function, sum, uses the window w. Since ORDER BY is specified, each record has
-- its place in the partition. The Frame clause is omitted, which means that the frame, RANGE
-- BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is applied. That means, the function calculates the
-- sum of the values for the records from the beginning of each year till the current month. It is
-- the cumulative total on an yearly basis.

-- The lag function returns the value of a given expression for the record, which is the given number of records before the current one (default is 1).
-- lead: This returns the value of a given expression evaluated for the record that is the given number of records after the current row.
-- first_value, last_value, nth_value: This returns the value of a given expression evaluated for the first record, last record, or nth record of the frame respectively.
-- row_number: This returns the number of the current row within the partition.
-- dense_rank: This returns the rank of the current row without gaps.
-- percent_rank and cume_dist: These return the relative rank of the current row. The difference is that the first function uses rank and the second uses row_number as a numerator for the calculations.
-- ntile: This divides the partition into the given number of equal parts and returns the integer number of the part where the current record belongs.

-- Since window functions are evaluated after grouping, it is possible to use aggregating functions
-- within the window functions, but not the other way around.

-- A code like the following is right:

sum( count(*) ) OVER()

-- This will also work: sum(a) OVER( ORDER BY count(*) )

-- However, the code, sum( count(*) OVER() ),is wrong.

-- For example, to calculate the rank of the seller accounts by the number of advertisements they
-- give, the following query can be used:

SELECT seller_account_id,
    dense_rank() OVER(ORDER BY count(*) DESC)
  FROM car_portal_app.advertisement
  GROUP BY seller_account_id;

----

-- PostgreSQL provides an explicit way of selecting the first record within each group. The DISTINCT
-- ON keywords are used for that. The syntax is as follows:

SELECT DISTINCT ON (<expression_list>) <Select-List>
...
ORDER BY <order_by_list>

-- In the preceding code snippet, for each distinct combination of values of expression_list, only
-- the first record will be returned by the SELECT statement. The ORDER BY clause is used to define
-- a rule to determine which record is the first. Expression_list from DISTINCT ON must be included
-- in the ORDER BY list.

-- For the task being discussed, it can be applied in the following way:

SELECT DISTINCT ON (car_id) advertisement_id,
    advertisement_date, car_id, seller_account_id
  FROM car_portal_app.advertisement
  ORDER BY car_id, advertisement_date;

----

SELECT * FROM generate_series('2015-01-01'::date, '2015-01-31'::date, interval '7 days');
--     generate_series     
-- ------------------------
--  2015-01-01 00:00:00-05
--  2015-01-08 00:00:00-05
--  2015-01-15 00:00:00-05
--  2015-01-22 00:00:00-05
--  2015-01-29 00:00:00-05
-- (5 rows)

----

-- The output of several set-returning functions can be combined together as if they were joined on
-- the position of each row. The ROWS FROM syntax is used for this, as follows:

ROWS FROM (function_call [,…]) [[AS] alias (column_name [,...])]

-- The preceding construct will return a relation. The number of rows is equal to the largest output
-- of the functions. Each column corresponds to the respective function in the ROWS FROM clause. If
-- a function returns fewer rows than other functions, the missing values will be set to NULL. For
-- example:

SELECT foo.a, foo.b FROM
  ROWS FROM (
    generate_series(1,3), generate_series(1,7,2)
  ) AS foo(a, b);
-- a | b
-- ---+---
--  1 | 1
--  2 | 3
--  3 | 5
--    | 7
-- (4 rows)

----

-- LATERAL SUBQUERIES

-- (((
-- https://blog.heapanalytics.com/postgresqls-powerful-new-join-type-lateral/
-- This is a bit dense. Loosely, it means that a LATERAL join is like a SQL foreach loop, in which
-- PostgreSQL will iterate over each row in a result set and evaluate a subquery using that row as a
-- parameter.
-- )))

-- Subqueries were discussed in the previous chapter. However, it is worth mentioning one specific
-- pattern of using them in more detail.

-- It is very convenient to use subqueries in the Select-list. They are used in the SELECT
-- statements, for example, to create calculated attributes when querying a table. Let's take the
-- car portal database again. Suppose it is necessary to query the table car to retrieve information
-- about the cars. For each car, it is required to assess its age by comparing it with the age of
-- the other cars of the same model. Furthermore, it is required to query the number of cars of the
-- same model.

-- These two additional fields can be generated by scalar subqueries, as follows:

  SELECT car_id, manufacture_year,
      CASE WHEN manufacture_year <=
        (SELECT avg(manufacture_year)
          FROM car_portal_app.car
          WHERE car_model_id = c.car_model_id
        ) THEN 'old' ELSE 'new' END as age,
      (SELECT count(*) FROM car_portal_app.car
        WHERE car_model_id = c.car_model_id
      ) AS same_model_count
    FROM car_portal_app.car c;

-- car_id | manufacture_year | age | same_model_count
-- --------+------------------+-----+------------------
--       1 |             2008 | old |                3
--       2 |             2014 | new |                6
--       3 |             2014 | new |                2
--   ...
-- (229 rows)

-- The power of those subqueries is that they can refer to the main table in their WHERE
-- clause. That makes them easy. It is also very simple to add more columns in the query by adding
-- other subqueries. On the other hand, there is a problem here: performance. The car table is
-- scanned by the database server once for the main query, and then it is scanned two times again
-- for each retrieved row, that is, for the age column and for the same_model_count column.

-- It is possible, of course, to calculate those aggregates once for each car model independently
-- and then join the results with the car table:

  SELECT car_id, manufacture_year,
      CASE WHEN manufacture_year <= avg_year
        THEN 'old' ELSE 'new' END as age,
      same_model_count
    FROM car_portal_app.car
      INNER JOIN (
        SELECT car_model_id,
            avg(manufacture_year) avg_year,
            count(*) same_model_count
          FROM car_portal_app.car
          GROUP BY car_model_id
      ) subq USING (car_model_id);

-- car_id | manufacture_year | age | same_model_count
-- --------+------------------+-----+------------------
--       1 |             2008 | old |                3
--       2 |             2014 | new |                6
--       3 |             2014 | new |                2
-- ...
-- (229 rows)

-- The result is the same and the query is 20 times faster. However, this query is only good for
-- retrieving many rows from the database. If it is required to get the information about only one
-- car, the first query will be faster.

-- One can see that the first query could perform better if it was possible to select two columns in
-- the subquery in the Select-list. But that is not possible. Scalar queries can return only one
-- column.

-- There is yet another way of using subqueries. It combines the advantages of the subqueries in the
-- Select-list, which can refer to the main table, with the subqueries in the FROM clause, which can
-- return multiple columns. This can be done via lateral subqueries that were added in PostgreSQL
-- version 9.3. Putting the LATERAL keyword before the subquery code in the FROM clause makes it
-- possible to reference any preceding items of the FROM clause from the subquery.

-- The query would be as follows:

SELECT car_id, manufacture_year,
        CASE WHEN manufacture_year <= avg_year
          THEN 'old' ELSE 'new' END as age,
        same_model_count
      FROM car_portal_app.car c,
        LATERAL (SELECT avg(manufacture_year) avg_year,
            count(*) same_model_count
          FROM car_portal_app.car
          WHERE car_model_id = c.car_model_id) subq;
-- car_id | manufacture_year | age | same_model_count
-- --------+------------------+-----+------------------
--       1 |             2008 | old |                3
--       2 |             2014 | new |                6
--       3 |             2014 | new |                2
-- ...
-- (229 rows)

-- This query is approximately two times faster than the first one, and is the best one for
-- retrieving only one row from the car table.

-- When it comes to set-returning functions, it is not necessary to use the LATERAL keyword. All
-- functions that are mentioned in the FROM clause can use the output of any preceding functions or
-- subqueries:

SELECT a, b FROM
  generate_series(1,3) AS a,
  generate_series(a, a+2) AS b;
-- a | b
-- ---+---
--  1 | 1
--  1 | 2
--  1 | 3
--  2 | 2
--  2 | 3
--  2 | 4
--  3 | 3
--  3 | 4
--  3 | 5
-- (9 rows)

-- In the preceding query, the first function that has the alias a returns three rows. For each of
-- those three rows, the second function is called, returning three more rows.

----

CREATE TABLE data(ID INT,cost_1 INT, quantity_1 INT, cost_2 INT, quantity_2 INT);

INSERT INTO data VALUES (1,1,2,3,4), (2,3,5,7,9),(3,10,5,20,2);

SELECT ID, cost_1, quantity_1, cost_2, quantity_2, total_1,total_2, total_1 + total_2 AS total_3 FROM data, LATERAL (SELECT cost_1 * quantity_1, cost_2 * quantity_2) AS s1(total_1,total_2);

--  id | cost_1 | quantity_1 | cost_2 | quantity_2 | total_1 | total_2 | total_3 
-- ----+--------+------------+--------+------------+---------+---------+---------
--   1 |      1 |          2 |      3 |          4 |       2 |      12 |      14
--   2 |      3 |          5 |      7 |          9 |      15 |      63 |      78
--   3 |     10 |          5 |     20 |          2 |      50 |      40 |      90
-- (3 rows)

----

-- > > Hi!
-- > > 
-- > > I'd like to do some calculation with values from the table, show them a
-- > > new column and use the values in a where-clause.
-- > > 
-- > > Something like this
-- > > select a, b , a*b as c from ta where c=2;
-- > > 
-- > > But postgresql complains, that column "c" does not exist.
-- > > 
-- > > Do I have to repeat the calculation (which might be even more complex
-- > > :-) ) in the "where"-clause, or is there a better way?
-- > > 
-- > > 
-- > > Thanks in advance.
-- > > 

-- Yes

----

SELECT * FROM account ORDER BY lower(first_name);
--  account_id |    first_name    |    last_name    |      email      | password  
-- ------------+------------------+-----------------+-----------------+-----------
--        1000 | test_first_name  | test_last_name  | test@email.com  | password
--        2000 | test_first_name2 | test_last_name2 | test2@email.com | password2
-- (2 rows)

SELECT first_name AS f FROM account ORDER BY lower(f);
-- ERROR:  column "f" does not exist
-- LINE 1: select first_name AS f from account order by lower(f);
--                                                            ^

SELECT first_name AS f FROM account ORDER BY f;
--         f         
-- ------------------
--  test_first_name
--  test_first_name2
-- (2 rows)

-- Quoting the manual page on SELECT:

-- Each expression can be the name or ordinal number of an output column (SELECT list item), or it
-- can be an arbitrary expression formed from input-column values.  You were trying to order by an
-- expression formed from an output-column, which is not possible.

-----

-- For example, the query regarding the distribution of the number of advertisements per car:

SELECT percentile_disc(ARRAY[0.25, 0.5, 0.75])
    WITHIN GROUP (ORDER BY cnt)
  FROM (
    SELECT count(*) cnt
      FROM car_portal_app.advertisement
      GROUP BY car_id) subq;

-- percentile_disc
-- -----------------
--  {2,3,5}
-- (1 row)

-- The result means that there are, at the most, two advertisements for 25 percent of the cars,
-- three advertisements for 50 percent of the cars, and five advertisements for 75 percent of the
-- cars in the database.

-- The syntax of the ordered-set aggregating functions differs from the normal aggregates and uses a
-- special construct, WITHIN GROUP (ORDER BY expression). The expression here is actually an
-- argument of a function. Here, not just the order of the rows but the values of the expressions as
-- well affect the result of the function. In contrast to the ORDER BY clause of the SELECT query,
-- only one expression is possible here, and no references to the output column numbers are allowed.


--

-- https://www.depesz.com/2014/01/11/waiting-for-9-4-support-ordered-set-within-group-aggregates/

--

-- For example, the following query gets the ID of the most frequent car model in the database:

SELECT mode() WITHIN GROUP (ORDER BY car_model_id)
FROM car_portal_app.car;
-- mode
-- ------
--    64

---- 

-- Another group of aggregates that use the same syntax are the hypothetical-set aggregating
-- functions. They are rank, dense_rank, percent_rank, and cume_dist. There are window functions
-- with the same names. Window functions take no argument and they return the result for the current
-- row. Aggregate functions have no current row because they are evaluated for a group of rows. But
-- they take an argument: the value for the hypothetical current row.

-- For example, the aggregate function rank returns the rank of a given value in the ordered set as
-- if that value existed in the set:

SELECT rank(2) WITHIN GROUP (ORDER BY a)  FROM generate_series(1,10,3) a;
-- rank
-- ------
--     2
-- (1 row)

-- In the preceding query, the value 2 does not exist in the output of generate_series (it returns
-- 1..4..7..10) . But if it existed, it would take the second position in the output.

----

SELECT
    is_deleted,
    min(entered_on),
    max(entered_on),
    count(*),
    percentile_disc( 0.5 ) WITHIN GROUP (ORDER BY entered_on)
FROM
    plans
GROUP BY
    is_deleted;

--  is_deleted |              min              |              max              | count |        percentile_disc        
-- ------------+-------------------------------+-------------------------------+-------+-------------------------------
--  f          | 2008-12-04 13:20:43+01        | 2013-12-16 22:56:49.951565+01 | 98687 | 2012-10-22 15:38:51.41854+02
--  t          | 2013-03-30 20:20:40.102483+01 | 2013-12-16 22:51:34.011972+01 |  6971 | 2013-08-20 07:47:46.051709+02
-- (2 rows)

----

-- For example, suppose it is required to count the number of cars in the database for each car
-- model separately, for each number of doors. If one groups the records by these two fields, the
-- result will be correct but not very convenient to use in reporting:

SELECT car_model_id, number_of_doors, count(*)
  FROM car_portal_app.car
  GROUP BY car_model_id, number_of_doors;

-- car_model_id | number_of_doors | count
-- --------------+-----------------+-------
--            47 |               4 |     1
--            42 |               3 |     2
--            76 |               5 |     1
--            52 |               5 |     2
-- ...

-- The FILTER clause makes the output much clearer:

SELECT car_model_id,
    count(*) FILTER (WHERE number_of_doors = 2) doors2,
    count(*) FILTER (WHERE number_of_doors = 3) doors3,
    count(*) FILTER (WHERE number_of_doors = 4) doors4,
    count(*) FILTER (WHERE number_of_doors = 5) doors5
  FROM car_portal_app.car
  GROUP BY car_model_id;

-- car_model_id | doors2 | doors3 | doors4 | doors5
-- --------------+--------+--------+--------+--------
--            43 |      0 |      0 |      0 |      2
--             8 |      0 |      0 |      1 |      0
--            11 |      0 |      2 |      1 |      0
--            80 |      0 |      1 |      0 |      0
-- ...

-- Note that the cars with a number of doors other that is than from 2 to 5 will not be counted by the query.

-- The same result can be achieved by calling functions, as follows:

count(CASE WHEN number_of_doors = 2 THEN 1 END) doors2

-- However, the FILTER clause method is easier and shorter.

----

-- isolation levels: Read Uncommitted, Read Committed, Repeatable Read, Serializable
-- Phenomena:        Dirty Read, Non-repeatable Read, Phantom-Read

-- Nonrepeatable read: When rereading data, a transaction can find that the data has been modified
-- by another transaction that has just committed. The same query executed twice can return
-- different values for the same rows.

-- Phantom read: This is similar to nonrepeatable read, but it is related to new data, created by
-- another transaction. The same query executed twice can return different numbers of records.

-- PostgreSQL does not explicitly support the Read uncommitted isolation level. Furthermore, Phantom
-- reads do not actually appear at the Repeatable read isolation level, which makes it quite similar
-- to Serializable. The actual difference between Repeatable read and Serializable is that
-- Serializable guarantees that the result of the concurrent transactions will be exactly the same
-- as if they were executed serially one after another, which is not always true for Repeatable
-- read.

-- PostgreSQL does not explicitly support the Read uncommitted isolation level. Furthermore, Phantom
-- reads do not actually appear at the Repeatable read isolation level, which makes it quite similar
-- to Serializable. The actual difference between Repeatable read and Serializable is that
-- Serializable guarantees that the result of the concurrent transactions will be exactly the same
-- as if they were executed serially one after another, which is not always true for Repeatable
-- read.

-- Another more complicated issue related to the concurrent access is serialization errors. Again,
-- suppose there are two transactions that run at the Serializable isolation level. Transaction A
-- updates a record, increasing the value of a field by 1. Transaction B tries to do the same with
-- the same record. Again, it will be blocked until transaction A is finished. If A is aborted, then
-- B will continue. But if A is committed, it will not allow B to continue. That is because both A
-- and B work with the snapshots of data at the moment of the start of the transactions. Both of
-- them will update a field from its original state and set it to the same new value. But the result
-- of these operations will be different from what could have been if the transactions were executed
-- one after another. In this situation, transaction B will fail with an error "could not serialize
-- access".

-- Database applications can manage concurrent access to shared resources by obtaining locks
-- explicitly. To lock a record or several records, an application should use the SELECT statement
-- with the locking clause. The locking clause, that is, FOR UPDATE is added to the very end of the
-- statement. This will not only return the records as any other SELECT statement, but also make
-- PostgreSQL lock those records against concurrent updates or deletions until the current
-- transaction is finished:

SELECT * FROM car_portal_app.car WHERE car_model_id = 5 FOR UPDATE;

-- This will lock all the records in the table car that satisfy the condition. If another instance
-- of the application tries to execute the same query, it will be blocked until the transaction is
-- committed or aborted. If an application does not want to wait, the locking clause can be
-- supplemented by the NOWAIT keyword. In that case, if a lock cannot be acquired, then the
-- statement will immediately report an error.

-- It is also possible to explicitly create table-level locks by using the SQL LOCK command.

----

SELECT ARRAY[1,2,3];
--   array  
-- ---------
--  {1,2,3}

-- But you can see that it's valid and to be expected because you can cast to an array explicitly.

SELECT '{1,2,3}'::int[];
--   int4   
-- ---------
--  {1,2,3}

----

-- The only way to change that display would be to change the output method for arrays in general. I
-- don't think this can be done without hacking the Postgres sources (and which would probably break
-- many things)

-- If you want to display arrays in a different format, write your own function to do that.

create function format_array(p_array anyarray)
  returns text
as
$$
  select translate(p_array::text, '{}', '[]');
$$
language sql;

-- Then you can use that on any array:

select format_array(array[1,2,3]), 
       format_array('{}'::int[]), 
       format_array(array[[1,2,3],[4,5,6]]);

-- will output:

-- format_array | format_array | format_array     
-- -------------+--------------+------------------
-- [1,2,3]      | []           | [[1,2,3],[4,5,6]]

----

-- The pg_class table is one of the main tables in pg_cataolg; it stores information about various relation types, as seen in the following list:

-- One could think of The Oversized-Attribute Storage Technique (TOAST) as a vertical partitioning
-- strategy. PostgreSQL does not allow tuples to span multiple pages where the page size is often 8
-- KB; therefore, PostgreSQL stores, breaks, and compresses large objects into several chunks and
-- stores them in other tables called TOAST tables.

----

car_portal=# SELECT 'car_portal_app.car'::regclass::oid;
--   oid
-- -------
--  24807
-- (1 row)

car_portal=# SELECT 24807::regclass::text;
--         text
-- --------------------
--  car_portal_app.car
-- (1 row)

-- Another approach is to use pg_class and pg_namespace to get the OID, as follows:

car_portal=# SELECT c.oid FROM pg_class c join pg_namespace n ON (c.relnamespace = n.oid) WHERE relname ='car' AND nspname ='car_portal_app';
--   oid
-- -------
--  24807
-- (1 row)

----

-- Finally, if one is not able to drop a certain database because clients try to connect to it, one
-- can execute the following query, which disallows users from connecting to the database and then
-- kills the connections:

UPDATE pg_database set datallowconn = 'false' WHERE datname = 'database to drop';

--- now kill the connections

---

-- pg_backend_pid() gives pid for your own backend

-- PostgreSQL provides the pg_terminate_backend(pid) and pg_cancel_backend(pid) functions, while
-- pg_cancel_backend only cancels the current query and pg_terminate_backend kills the entire
-- connection. The following query terminates all connections to the current database except the
-- session connection:

SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();

----

-- Getting a value can be done in several ways, such as selecting the value from the pg_settings
-- catalog view, browsing the postgresql.conf file, or using the following function and statement:

car_portal=# SELECT current_setting('work_mem');
--  current_setting
-- -----------------
--  4MB
-- (1 row)

car_portal=# show work_mem;
--  work_mem
-- ----------
--  4MB
-- (1  row)

-- In order to change a certain configuration value, one could use the set_config(settin_name, setting_value, scope) function, as follows:

car_portal=# SELECT set_config('work_mem', '8 MB', true); -- true means only current transaction is affected
--  set_config
-- ------------
--  8MB
-- (1 row)

-- If one wants to use the preceding function to set a certain value for the whole session, the true value should be replaced with false.

-- Also, note that the set_config function can raise an error if the setting cannot be set, as follows:

car_portal=# SELECT set_config('shared_buffers', '1 GB', false);
-- ERROR:  parameter "shared_buffers" cannot be changed without restarting the server

----

-- The ALTER SYSTEM command is used to change PostgreSQL configuration parameters. The synopsis for ALTER SYSTEM is:

ALTER SYSTEM SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }

ALTER SYSTEM RESET configuration_parameter
ALTER SYSTEM RESET ALL

-- In case the setting value requires a system reload or restart, the setting will take effect after
-- the system reload or restart, respectively. The ALTER SYSTEM command requires superuser
-- privileges.

-- Finally, browsing the postgresql.conf file is a bit tricky due to the big number of postgres
-- settings. Also, most settings have the default boot values; therefore, getting the server
-- configuration settings that are not assigned the default values in postresql.conf can be easily
-- done, as follows:

car_portal=# SELECT name, current_setting(name), source FROM pg_settings WHERE source IN ('configuration file');
--             name            |       current_setting       |       source
-- ----------------------------+-----------------------------+--------------------
--  DateStyle                  | ISO, DMY                    | configuration file
--  default_text_search_config | pg_catalog.english          | configuration file

----

car_portal=# select oid, datname from pg_database;
--   oid  |  datname   
-- -------+------------
--  12669 | postgres
--  16385 | car_portal
--      1 | template1
--  12668 | template0
-- (4 rows)

-- du -h /usr/local/var/posgres/base/<oid>

SELECT pg_database.datname, pg_size_pretty(pg_database_size(pg_database.datname)) AS size FROM pg_database;
--   datname   |  size
-- ------------+---------
--  template1  | 6417 kB
--  template0  | 6409 kB
--  postgres   | 6540 kB
--  test       | 6532 kB
--  car_portal | 29 MB
-- (1  rows)

-- One can get the table size, including indexes and toast tables, using the pg_totoal_relation_size
-- function. If one is interested only in the table size, one can use the pg_relation_size
-- function. This information helps manage table growth as well as table spaces. Take a look at the
-- following query:

car_portal=# SELECT tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) FROM pg_tables WHERE schemaname = 'car_portal_app' LIMIT 2;
--        tablename        | pg_size_pretty
-- ------------------------+----------------
--  advertisement_rating   | 16 kB
--  log                    | 48 kB
-- (1  rows)

-- Finally, to get the index size, one could use the pg_relation_size function, as follows:

car_portal=# SELECT indexrelid::regclass,  pg_size_pretty(pg_relation_size(indexrelid::regclass))  FROM pg_index WHERE indexrelid::regclass::text like 'car_portal_app.%' limit 2;
--                               indexrelid                              | pg_size_pretty
-- ----------------------------------------------------------------------+----------------
--  car_portal_app.account_email_key                                     | 16 kB
--  car_portal_app.account_history_account_id_search_key_search_date_key | 8192 bytes
-- (1  rows)

----

-- the following query shows only two empty tables:

car_portal=# SELECT relname FROM pg_stat_user_tables WHERE n_live_tup= 0 limit 2;
--         relname
-- -----------------------
--  logged_actions
--  advertisement_picture
-- (1  rows)

-- To find the empty columns, one can have a look at the null_fraction attribute of the pg_stats
-- table, as follows:

car_portal=# SELECT schemaname, tablename, attname FROM pg_stats WHERE null_frac= 1 and schemaname NOT IN ('pg_catalog', 'information_schema') limit 1;
--    schemaname   | tablename | attname
-- ----------------+-----------+---------
--  car_portal_app | log       | old_row
-- (1 row)

----

-- For views, there are no statistics collected unless the views are materialized. In order to
-- assess whether a view is used, we need to do this manually. This can be done by rewriting the
-- view and joining it with a function with a certain side effect, such as updating a table and
-- increasing the number of times the view is accessed or raising a certain log message. To test
-- this technique, let's write a simple function that raises a log, as follows:

CREATE OR REPLACE FUNCTION monitor_view_usage (view_name TEXT) RETURNS BOOLEAN AS $$
BEGIN
  RAISE LOG 'The view % is used on % by % ', view_name, current_time, session_user;
  RETURN TRUE;
END;
$$
LANGUAGE plpgsql
cost .001;

-- Now, let's assume that we want to drop the following view; however, there is uncertainty
-- regarding whether an application depends on it:

CREATE OR REPLACE VIEW sport_car AS
SELECT car_id, number_of_owners,regestration_number
FROM car_portal_app.car
WHERE number_of_doors = 3;

-- To ensure that the view is not used, the view should be rewritten as follows, where the
-- monitor_view_usage function is used, and the log files should be monitored for a certain period
-- of time. The following query can help:

CREATE OR REPLACE VIEW sport_car AS SELECT car_id, number_of_owners, regestration_number
FROM car_portal_app.car CROSS JOIN monitor_view_usage ('sport_car')
WHERE number_of_doors = 3;

-- If the view is used, an entry in the log file should appear, as follows:

-- 2015-06-03 17:55:04 CEST LOG:  The view sport_car is used on 17:55:04.571+02 by postgres.

----


-- The first step is to identify the tables that do not have unique and primary key constraints. This is quite easy using the information schema, as follows:

SELECT table_catalog, table_schema, table_name
FROM
  information_schema.tables
WHERE
  table_schema NOT IN ('information_schema', 'pg_catalog')
EXCEPT
SELECT
  table_catalog, table_schema, table_name
FROM
  information_schema.table_constraints
WHERE
  constraint_type IN ('PRIMARY KEY', 'UNIQUE') AND
  table_schema NOT IN ('information_schema', 'pg_catalog');

-- The second step is to identify the tables that really contain duplicates; this can be performed by aggregating the data on the table. To check this, let's create a table with some duplicates in it, as follows:

CREATE TABLE duplicate AS SELECT (random () * 9  + 1)::INT as f1 , (random () * 9 + 1)::INT as f2 FROM generate_series (1,20);

SELECT count(*), f1, f2 FROM duplicate GROUP BY f1, f2 having count(*) > 1 limit 3;
--  count | f1 | f2
-- -------+----+----
--      3 |  7 |  7
--      2 |  7 |  2
--      2 |  7 |  6
-- (3 rows)

-- The tricky part is to delete the duplicates as the rows are identical. To delete duplicates, a
-- certain row needs to be marked to stay, and the rest need to be deleted. This can be achieved
-- using the ctid column. In PostgreSQL, each row has a header, and the ctid column is the physical
-- location of the row version within the table. Thus, the ctid column can be used as a row
-- identifier temporarily because this identifier may change after running maintenance commands,
-- such as CLUSTER.

-- To delete the duplicate, one can perform the following query:

with should_not_delete as (
  SELECT min(ctid) FROM duplicate group by f1, f2
) DELETE FROM duplicate WHERE ctid NOT IN (SELECT min FROM should_not_delete);

----

-- There are several other approaches to clean up duplicate rows. For example, one can use the
-- CREATE TABLE and SELECT DISTINCT statements to create a table with a unique set of rows. Then,
-- one can drop the original table and rename the created table after the original table, as shown
-- in the following example:

CREATE TABLE <tmp> AS SELECT DISTINCT * FROM <orig_tbl>;
DROP TABLE <orig_tbl>;
ALTER TABLE <tmp> RENAME TO <orig_tbl>;

-- Note that this approach might be faster than the approach represented in the preceding example;
-- however, this technique may not work if there are other objects depending on the table that needs
-- to be dropped, such as views, indexes, and so on.

-- If the table that contains duplicate records has a primary key, one can drop the table using the
-- DELETE…USING statement, as follows:

DELETE FROM dup_table a USING dup_table b
WHERE a.tt1 = b.tt1 AND ... AND b.attn= b.attn
AND a.pk < p.pk.

-- The list of attributes, att1,..., attn, is used to join the table with itself, and the primary
-- key indicated in the code as pk is used to specify the record that needs to be deleted. Note that
-- this approach is often faster than the first approach as it does not require aggregation.

---

-- In the case of idle in-transaction queries, one could detect the locks using the pg_stat_activity
-- and pg_lock tables, as follows:

-- Note the use of ON(...) IS NOT DISTINCT FROM (...)

SELECT
  lock1.pid as locked_pid,
  stat1.usename as locked_user,
  stat1.query as locked_statement,
  stat1.state as state,
  stat2.query as locking_statement,
  stat2.state as state,
  now() - stat1.query_start as locking_duration,
  lock2.pid as locking_pid,
  stat2.usename as locking_user
FROM pg_catalog.pg_locks lock1
     JOIN pg_catalog.pg_stat_activity stat1 on lock1.pid = stat1.pid
     JOIN pg_catalog.pg_locks lock2 on
  (lock1.locktype,lock1.database,lock1.relation, lock1.page,lock1.tuple,lock1.virtualxid, lock1.transactionid,lock1.classid,lock1.objid, lock1.objsubid) IS NOT DISTINCT FROM
        (lock2.locktype,lock2.DATABASE, lock2.relation,lock2.page, lock2.tuple,lock2.virtualxid, lock2.transactionid,lock2.classid, lock2.objid,lock2.objsubid)
     JOIN pg_catalog.pg_stat_activity stat2 on lock2.pid = stat2.pid
WHERE NOT lock1.granted AND lock2.granted;

----

-- Always index foreign keys: Indexing a table on a foreign key allows PostgreSQL to fetch data from
-- the table using an index scan.

-- Increase the column statistic target on foreign keys: This is also applicable to all predicates
-- because it allows PostgreSQL to have a better estimation of the number of rows. The default
-- statistic target is 100, and the maximum is 10,000. Increasing the statistics target makes the
-- ANALYZE command slower.


----

-- To generate this tree of dependency, one can execute the following queries:

CREATE VIEW a AS SELECT 1 FROM car;
CREATE VIEW b AS SELECT 1 FROM a;
CREATE VIEW c AS SELECT 1 FROM a;
CREATE VIEW d AS SELECT 1 FROM b,c;
CREATE VIEW e AS SELECT 1 FROM c;
CREATE VIEW f AS SELECT 1 FROM d,c;

-- To get the views and find out how they depend on each other in the preceding queries, the following query can be used:

SELECT view_schema,view_name parent, table_schema, table_name  FROM information_schema.view_table_usage WHERE view_name LIKE '_' order by view_name;
--  view_schema | parent | table_schema | table_name
-- -------------+--------+--------------+------------
--  public      | a      | public       | car
--  public      | b      | public       | a
--  public      | c      | public       | a
--  public      | d      | public       | c
--  public      | d      | public       | b
--  public      | e      | public       | c
--  public      | f      | public       | c
--  public      | f      | public       | d
-- (8 rows)

-- Now, to solve the dependency tree, a recursive query will be used, as follows:

CREATE OR REPLACE FUNCTION get_dependency (schema_name text, view_name text) RETURNS TABLE (schema_name text, view_name text, level int) AS $$
WITH RECURSIVE view_tree(parent_schema, parent_view, child_schema, child_view, level) as
(
  SELECT
  parent.view_schema,
  parent.view_name ,
  parent.table_schema,
  parent.table_name,
  1
  FROM
  information_schema.view_table_usage parent
  WHERE
  parent.view_schema = $1 AND
  parent.view_name = $2
  UNION ALL
  SELECT
  child.view_schema,
  child.view_name,
  child.table_schema,
  child.table_name,
  parent.level + 1
  FROM
  view_tree parent JOIN information_schema.view_table_usage child ON child.table_schema = parent.parent_schema AND child.table_name = parent.parent_view
)
SELECT DISTINCT
  parent_schema,
  parent_view,
  level
FROM
  (SELECT
    parent_schema,
    parent_view,
    max (level) OVER (PARTITION BY parent_schema, parent_view) as max_level,
    level
  FROM   
    view_tree) AS FOO
WHERE level = max_level;
$$
LANGUAGE SQL;

-- In the preceding query, the inner part of the query is used to calculate dependency levels, while
-- the outer part of the query is used to eliminate duplicates. The following shows the dependencies
-- for view a in the right order:

car_portal=# SELECT * FROM get_dependency ('public', 'a') ORDER BY Level;
--  schema_name | view_name | level
-- -------------+-----------+-------
--  public      | a         |     1
--  public      | b         |     2
--  public      | c         |     2
--  public      | d         |     3
--  public      | e         |     3
--  public      | f         |     4
-- (6 rows)


-- To dump the view's definition, one can use pg_dump with the -t option, which is used to dump a certain relation. So, to dump the views in the previous example, one can use the following trick:

-- pg_dump –s $(psql -t car_portal -c "SELECT string_agg (' -t ' ||
-- quote_ident(schema_name)||'.'||quote_ident(view_name), ' ' ORDER BY level ) FROM get_dependency
-- ('public'::text, 'a'::text)" ) -d car_portal>/tmp/dump.sql

-- The psql uses the -t option to return tuples, and the string aggregate function is used to
-- generate the list of views that need to be dumped based on the level order. So, the inner psql
-- query gives the following output:

-- -t public.a  -t public.b  -t public.c  -t public.d  -t public.e  -t public.f

----


-- Shared buffers (shared_buffers): The default value for shared buffers is 32 MB; however, it is
-- recommended to set it around 25 percent of the total memory, but not more than 8 GB on Linux
-- systems and 512 MB on windows system. Sometimes, increasing shared_buffers to a very high value
-- leads to an increase in performance because the database can be cached completely in the
-- RAM. However, the drawback of increasing this value too much is that one can't allocate memory
-- for CPU operations such as sorting and hashing.

-- Working memory (work_mem): The default value is 1 MB; for CPU-bound operations, it is important
-- to increase this value. The work_mem setting is linked with the number of connections, so the
-- total amount of RAM used equals the number of connections multiplied by work_mem. Working memory
-- is used to sort and hash, so it affects the queries that use the ORDER BY, DISTINCT, UNION, and
-- EXCEPT constructs. To test your working method, you could analyze a query that uses sort and take
-- a look at whether the sort operation uses the memory or hard disk, as follows:

EXPLAIN ANALYZE SELECT n FROM generate_series(1,5) as foo(n) order by n;
-- Sort  (cost=59.83..62.33 rows=1000 width=4) (actual time=0.075..0.075 rows=5 loops=1)
--   Sort Key: n
--   Sort Method: quicksort  Memory: 25kB
--   ->  Function Scan on generate_series foo  (cost=0.00..10.00 rows=1000 width=4) (actual time=0.018..0.018 rows=5 loops=1)"
-- Total runtime: 0.100 ms

----

-- Also, a checkpoint_segment of small value might also lead to performance penalty in write-heavy
-- systems; on the other hand, increasing the checkpoint_segments setting to a high value will
-- increase recovery time.

-- In specific cases, such as bulk upload operations, performance can be increased by altering hard
-- disk settings, changing the logging configuration to log minimal info, and finally disabling auto
-- vacuuming; however, after the bulk operation is over, one should not forget to reset the server
-- configurations and run the VACUUM ANALYZE command.

-- Planner-related settings

-- Effective cache size (effective_cache_size) should be set to an estimate of how much memory is
-- available for disk caching in the operating system and within the database after taking into
-- account what is used by the operating system and other applications. This value for a dedicated
-- postgres server is around 50 percent to 70 percent of the total RAM. Also, one could play with a
-- planner setting, such as random_page_cost, to favor index scan over sequential scans. The
-- random_page_cost setting's default value is 4.0. In high-end SAN/NAS technologies, one could set
-- this value to 3, and for SSD, one could use a random page cost of 1.5 to 2.5.

-- The preceding list of parameters is minimal; in reality, one needs to also configure the logging,
-- checkpoint, wal, and vacuum settings. Also, note that some parameters cannot be changed easily on
-- production systems because they require a system restart, such as max_connections,
-- shared_buffers, fsync, and checkpoint_segments. In other cases, such as work_mem, it can be
-- specified in the session, giving the developer the option of tuning queries that need specific
-- work_mem.

-- Benchmarking is your friend

-- pgbench is a simple program used to execute a prepared set of SQL commands to calculate the
-- average transaction rate (transactions per second). pgbench is an implementation of the
-- Transaction Processing Performance Council (TPC) TPC-B standard. pgbench can also be customized
-- with scripts. In general, when using a benching framework, one needs to set it up on a different
-- client in order not to steal the RAM and CPU from the tested server. Also, one should run pgbench
-- several times with different load scenarios and configuration settings.

-- Finally, in addition to pgbench, there are several open source implementations for different
-- benchmarking standards, such as TPC-C and TPC-H.

-- The pgbench synopsis is as follows:

-- pgbench [options] dbname

-- The -i option is used to initialize the database with test tables, and the -s option determines
-- the database scale factor, also known as the number of rows in each table. A default output of
-- Pgbench using a default scale factor on a virtual machine with one CPU looks similar to the
-- following:

-----

-- TPC-C simulates a complete computing environment where a population of users executes
-- transactions against a database. The benchmark is centered around the principal activities
-- (transactions) of an order-entry environment. These transactions include entering and delivering
-- orders, recording payments, checking the status of orders, and monitoring the level of stock at
-- the warehouses.

-- Summary In August 1990, the TPC approved its second benchmark, TPC-B. In contrast to TPC-A, TPC-B
-- is not an OLTP benchmark. Rather, TPC-B can be looked at as a database stress test, characterized
-- by:

-- The TPC Benchmark™H (TPC-H) is a decision support benchmark. It consists of a suite of business
-- oriented ad-hoc queries and concurrent data modifications. The queries and the data populating
-- the database have been chosen to have broad industry-wide relevance. This benchmark illustrates
-- decision support systems that examine large volumes of data, execute queries with a high degree
-- of complexity, and give answers to critical business questions.

----

-- For beginners, it is extremely useful to write the same query in different ways and compare the
-- results. For example, in some cases, the NOT IN construct can be converted to LEFT JOIN or NOT
-- EXIST. Also, the IN construct can be rewritten using INNER JOIN as well as EXISTS. Writing the
-- query in several ways teaches the developer when to use or avoid a certain construct and what the
-- conditions that favor a certain construct are. In general, the NOT IN construct can sometimes
-- cause performance issues because postgres cannot use indexes to evaluate the query.

-- Another important issue is to keep tracking the new SQL commands and features. The PostgreSQL
-- development community is very active, and their contributions are often targeted to solving
-- common issues. For example, the LATERAL JOIN construct, which was introduced in PostgreSQL 9.3,
-- can be used to optimize certain GROUP BY and LIMIT scenarios.

----

SELECT a FROM table1 WHERE a NOT IN (SELECT a FROM table2)

SELECT a FROM table1 WHERE NOT EXISTS (SELECT * FROM table2 WHERE table1.a = table2.a)

SELECT a FROM table1 LEFT JOIN table2 ON table1.a = table2.a WHERE table1.a IS NULL


-- If the database is good at optimising the query, the two first will be transformed to something close to the third.

-- For simple situations like the ones in you question, there should be little or no difference, as
-- they all will be executed as joins. In more complex queries, the database might not be able to
-- make a join out of the not in and not exists queryes. In that case the queries will get a lot
-- slower. On the other hand, a join may also perform badly if there is no index that can be used,
-- so just because you use a join doesn't mean that you are safe. You would have to examine the
-- execution plan of the query to tell if there may be any performance problems.

--

-- Assuming you are avoiding nulls, they are all ways of writing an anti-join using Standard SQL.

-- An obvious omission is the equivalent using EXCEPT:

SELECT a FROM table1
EXCEPT
SELECT a FROM table2
Note in Oracle you need to use the MINUS operator (arguably a better name):

SELECT a FROM table1
MINUS
SELECT a FROM table2

----

-- Note that the number of returned rows is extremely high, as compared to the real number as
-- well. If this query were a subquery of another query, the error would be cascaded because it
-- might be executed several times.

----

-- EXPLAIN (ANALYZE, BUFFERS)

----


-- I think answer 4 is correct. There are a few considerations:

-- type of subquery - is it corrrelated, or not. Consider:

SELECT *
FROM   t1
WHERE  id IN (
             SELECT id
             FROM   t2
            )

-- Here, the subquery is not correlated to the outer query. If the number of values in t2.id is
-- small in comparison to t1.id, it is probably most efficient to first execute the subquery, and
-- keep the result in memory, and then scan t1 or an index on t1.id, matching against the cached
-- values.

-- But if the query is:

SELECT *
FROM   t1
WHERE  id IN (
             SELECT id
             FROM   t2
             WHERE  t2.type = t1.type
            )

-- here the subquery is correlated - there is no way to compute the subquery unless t1.type is
-- known. Since the value for t1.type may vary for each row of the outer query, this subquery could
-- be executed once for each row of the outer query.

-- Then again, the RDBMS may be really smart and realize there are only a few possible values for
-- t2.type. In that case, it may still use the approach used for the uncorrelated subquery if it can
-- guess that the cost of executing the subquery once will be cheaper that doing it for each row.

----

CREATE OR REPLACE FUNCTION generate_random_text ( int  ) RETURNS TEXT AS
$$
SELECT string_agg(substr('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', trunc(random() * 62)::integer + 1, 1), '')  FROM  generate_series(1, $1)
$$
LANGUAGE SQL;

----

-- Using functions on constant arguments also causes the index to be used if this function is not volatile as the optimizer evaluates the function as follows:

EXPLAIN SELECT * FROM login WHERE login_name = lower('jxaG6gjJ');
-- Index Scan using login_login_name_idx on login  (cost=0.28..8.29 rows=1 width=13)
--   Index Cond: (login_name = 'jxag6gjj'::text)
-- Using functions on columns, as stated in the preceding example, causes a sequential scan, as shown in the following query:

EXPLAIN SELECT * FROM login WHERE lower(login_name) = lower('jxaG6gjJ');
-- "Seq Scan on login  (cost=0.00..21.00 rows=5 width=13)"
-- "  Filter: (lower(login_name) = 'jxag6gjj'::text)"

-- Note that here, the number of rows returned is also five as the optimizer cannot evaluate the
-- predict correctly. To solve this issue, simply add an index, as follows:

CREATE INDEX ON login(lower(login_name));
EXPLAIN SELECT * FROM login WHERE lower(login_name) = lower('jxaG6gjJ');
-- Index Scan using login_lower_idx on login  (cost=0.28..8.29 rows=1 width=13)"
--   Index Cond: (lower(login_name) = 'jxag6gjj'::text)

-- Also, text indexing is governed by the access pattern. In general, there are two ways to index
-- text: the first approach is to use an index with opclass, which allows anchored text search, and
-- the second approach is to use tsquery and tsvector. In the test_explain_1 table, one could create
-- an opclass index, as follows:

SELECT * FROM test_explain_1 WHERE name like 'a%';
-- Time: 19.565 ms
CREATE INDEX on test_explain_1 (name text_pattern_ops);
SELECT * FROM test_explain_1 WHERE name like 'a%';
-- Time: 7.860 ms
EXPLAIN SELECT * FROM test_explain_1 WHERE name like 'a%';
--                                         QUERY PLAN
-- -------------------------------------------------------------------------------------------
--  Bitmap Heap Scan on test_explain_1  (cost=281.22..1204.00 rows=7071 width=37)
--    Filter: (name ~~ 'a%'::text)
--    ->  Bitmap Index Scan on test_explain_1_name_idx  (cost=0.00..279.45 rows=7103 width=0)
--          Index Cond: ((name ~>=~ 'a'::text) AND (name ~<~ 'b'::text))
-- (4 rows)

-- Note the execution plan after creating the index; an index scan was used instead of a sequential scan, and performance increased.

----

-- Often, one can see queries that cause several table scans if one uses a select statement in a
-- select list or the query is not well written. For example, let's assume that there is a query to
-- get the count of bad and good ranking as per a certain advertisement, which is written as
-- follows:

SELECT
(SELECT count(*) FROM car_portal_app.advertisement_rating WHERE rank = 1 AND advertisement_rating_id = 1) AS good,
(SELECT COUNT(*) FROM car_portal_app.advertisement_rating WHERE rank = 5 AND advertisement_rating_id = 1) AS bad;

-- This query caused the same index to be scanned twice, as shown in the execution plan. Take a look at the following query:

-- Result  (cost=16.36..16.38 rows=1 width=0)
--    InitPlan 1 (returns $0)
--      ->  Aggregate  (cost=8.17..8.18 rows=1 width=0)
--            ->  Index Scan using advertisement_rating_pkey on advertisement_rating  (cost=0.15..8.17 rows=1 width=0)
--                  Index Cond: (advertisement_rating_id = 1)
--                  Filter: (rank = 1)
--    InitPlan 2 (returns $1)
--      ->  Aggregate  (cost=8.17..8.18 rows=1 width=0)
--            ->  Index Scan using advertisement_rating_pkey on advertisement_rating advertisement_rating_1  (cost=0.15..8.17 rows=1 width=0)
--                  Index Cond: (advertisement_rating_id = 1)
--                  Filter: (rank = 5)

-- The preceding query could be written using COUNT FILTER or COUNT CASE expression END, as follows:

SELECT count(*) FILTER (WHERE rank=5) as bad, count(*) FILTER (WHERE rank=1) as good FROM car_portal_app.advertisement_rating WHERE advertisement_rating_id = 1;

----


-- Using correlated nested queries

-- Correlated nested queries can cause performance issues because the subquery is executed within a
-- loop; for example, the following query is not optimal:

CREATE TABLE test_explain_2 AS SELECT n as id, md5(n::text) as name FROM generate_series(1, 1000) as foo(n);
SELECT 1000
-- Time: 14.161 ms
SELECT * FROM test_explain_1 WHERE EXISTS (SELECT 1 FROM test_explain_2 WHERE id = id);
-- Time: 78.533 ms

-- The preceding query could be written using the INNER JOIN or IN construct, as follows:

# SELECT test_explain_1.* FROM test_explain_1 INNER JOIN  test_explain_2 USING (id);
-- Time: 2.111 ms
SELECT * FROM test_explain_1 WHERE id IN (SELECT id FROM test_explain_2);
-- Time: 2.143 ms

----

-- However, using a CTE may be problematic in the case of predicate push down as PostgreSQL does not
-- optimize beyond CTE boundaries; each CTE runs in isolation. To understand this limitation, let's
-- have a look at the following two dummy-equivalent examples and note the difference between their
-- performance:

car_portal=# With test_explain AS (SELECT * FROM test_explain_1) SELECT * FROM test_explain WHERE id = 4;
--  id |               name
-- ----+----------------------------------
--   4 | aa0cca507bdb343206f579cab2a46169
-- (1 row)

-- Time: 82.280 ms

SELECT * FROM (SELECT * FROM test_explain_1) as foo WHERE id = 4;
--  id |               name
-- ----+----------------------------------
--   4 | aa0cca507bdb343206f579cab2a46169
-- (1 row)
-- Time: 0.643 ms

----

-- index scan, sequential scan, not equals predicates
--
-- https://stackoverflow.com/questions/2864267/sql-indexes-for-not-equal-searches

----

-- cross column correlation leads to wrong estimation of rows
-- rows = rows  * selectivity of col1 * selectivity of col2 (in case of where col1=<> and col2 = <>
-- https://stackoverflow.com/questions/25812345/how-cross-column-correlation-is-handled-by-postgresql-optimizer
-- https://stackoverflow.com/questions/25812345/how-cross-column-correlation-is-handled-by-postgresql-optimizer

----

Table partitioning

Table partitioning is used to increase performance by physically arranging data in the hard disk based on a certain grouping criteria. There are two techniques for table partitioning:

Vertical table partitioning: The table is divided into several tables in order to decrease the row size. This allows a faster sequential scan on divided tables as a relation page holds more rows. To explain, let's assume that we want to store pictures for each seller in the database to be used as their respective logos. One could model this by adding a column of the bytea or blob type. The other approach is to have another table reference the sellers table, as follows:
CREATE TABLE car_portal_app.seller_logo (
  seller_id INT PRIMARY KEY REFERENCES car_portal_app.seller_account (seller_account_id),
  logo bytea NOT NULL
);
Horizontal table partitioning: This is used to decrease the whole table size by splitting the rows over multiple tables; it is supported by table inheritance and constraint exclusion.
In horizontal table partitioning, the parent table is often a proxy, while the child tables are used to store actual data. Table inheritance can be used to implement horizontal table partitioning, whereas constraint exclusion is used to optimize performance by only accessing the child tables that contain required data when performing a query. In general, when one wants to create table partitioning, he/she needs to do the following:

Create a parent table, which will act as proxy
Create child tables, where data will be saved
Create a trigger or rules on the master table to correctly save data in child tables based on partition criteria
Create check constraints in child tables to speed up the process of querying data
To understand table partitioning and constraint exclusion, let's assume that we have a log table where each entry has log_type, as follows:

CREATE TABLE log (
  log_id SERIAL PRIMARY KEY,
  log_information JSONB,
  log_type CHAR(1) 
);
Also, let's assume that we want to partition the log table based on log_type using this naming convention: log_u for update, log_d for delete, and log_i for insert. The child tables can be created as follows:

CREATE TABLE log_u ( CHECK ( log_type = 'u') ) INHERITS (log);
CREATE TABLE log_i ( CHECK ( log_type = 'i') ) INHERITS (log);
CREATE TABLE log_d ( CHECK ( log_type = 'd') ) INHERITS (log);
To store data in a child table, a trigger on the log table should be created, as follows:

CREATE OR REPLACE FUNCTION log_insert() RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.log_type = 'u' ) THEN
        INSERT INTO log_u VALUES (NEW.*);
    ELSIF ( NEW.log_type = 'i' ) THEN
        INSERT INTO log_i VALUES (NEW.*);
     ELSIF ( NEW.log_type = 'd' ) THEN
        INSERT INTO log_d VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Unknown log type';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_insert
    BEFORE INSERT ON log
    FOR EACH ROW EXECUTE PROCEDURE log_insert();
Note that the trigger execution time is before, and it returns NULL; thus, no rows are inserted on the master table, and the message INSERT 0 0 is shown. This causes some ORM, such as hibernate, to not function properly because when data is inserted or updated, the right number of affected rows is not returned. One can overcome this limitation using rules.

To test the table partitioning, let's insert three records with different types, as follows:

car_portal=# INSERT INTO log (log_information, log_type) VALUES ('{"query": "SELECT 1", "user":"x" }', 'i');
INSERT 0 0
car_portal=# INSERT INTO  log (log_information, log_type) VALUES ('{"query": "UPDATE ...", "user":"x" }', 'u');
INSERT 0 0
car_portal=# INSERT INTO  log (log_information, log_type) VALUES ('{"query": "DELETE ...", "user":"x" }', 'd');
INSERT 0 0 car_portal=# VACUUM ANALYSE LOG;
VACUUM
To test the constraint exclusion, let's check the execution plan of selecting from the log table all entries with log_type='i', is as follows:

car_portal=# EXPLAIN SELECT * FROM log WHERE log_type='i';
                         QUERY PLAN
------------------------------------------------------------
 Append  (cost=0.00..1.01 rows=2 width=44)
   ->  Seq Scan on log  (cost=0.00..0.00 rows=1 width=44)
         Filter: (log_type = 'i'::bpchar)
   ->  Seq Scan on log_i  (cost=0.00..1.01 rows=1 width=45)
         Filter: (log_type = 'i'::bpchar)
(5 rows)
Note in the preceding execution plan that two tables are scanned: the parent and child tables with the name log_i. This is the expected behavior of constraint exclusion as check constraint CHECK ( log_type = 'i') matches the log_type='i' predicate in a select statement.

Constraint exclusion limitations

Sometimes, constraint exclusion fails to kick in, leading to very slow queries. There are limitations on constraint exclusion, which are as follows:

The constraint exclusion setting can be disabled
Constraint exclusion works only on range and equality expressions
Constraint exclusion does not work if the where expression is not written in the equality or range manner
Let's assume that we want to partition a table based on text pattern, such as "pattern LIKE 'a%'". This can be achieved by rewriting the LIKE construct using range equality such as pattern >='a ' and pattern < 'b'. So, instead of having a check constraint on a child table using the LIKE construct, one should have it based on ranges. Also, if a user performs a select statement using the LIKE construct, constraint exclusion will not work.

