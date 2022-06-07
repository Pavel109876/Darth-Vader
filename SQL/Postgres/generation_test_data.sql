CREATE SEQUENCE test_1 INCREMENT BY 1 MINVALUE 1  NO MAXVALUE 

CREATE TABLE abc (x1 int, x2 date not null default CURRENT_DATE, x3 TEXT NOT NULL default md5(random()::text))

SELECT * FROM abc WHERE x3 LIKE '%' || '123' || '%';

--DROP PROCEDURE insert_data()

CREATE PROCEDURE insert_data()
LANGUAGE plpgsql AS
$$
DECLARE
x1 integer := 10000000;
BEGIN
WHILE x1 > 0 LOOP
INSERT INTO abc(x1) VALUES (nextval('test_1'));
x1 := x1 - 1;
END LOOP;
END;
$$;

CALL insert_data();