CREATE TABLE ANIMAL (
                      ID     BIGSERIAL    NOT NULL PRIMARY KEY,
                      NAME   VARCHAR(100) NOT NULL UNIQUE,
                      IS_PET BOOLEAN      NOT NULL);

INSERT INTO ANIMAL (IS_PET, NAME) VALUES (TRUE, 'dog');
INSERT INTO ANIMAL (IS_PET, NAME) VALUES (TRUE, 'cat');
INSERT INTO ANIMAL (IS_PET, NAME) VALUES (FALSE, 'cow');

CREATE FUNCTION animal_stats(is_pet BOOLEAN, OUT count BIGINT, OUT perc REAL) AS '
BEGIN
  count:= (SELECT count(*)
           FROM ANIMAL);
  perc:= (SELECT 100 * CAST(count(*) AS REAL) / count
          FROM ANIMAL
          WHERE ANIMAL.IS_PET = animal_stats.is_pet);
END;' LANGUAGE plpgsql;

CREATE FUNCTION f_inout_inout_inout(INOUT b1 BOOLEAN, INOUT b2 BOOLEAN, INOUT b3 BOOLEAN) AS '
BEGIN
  b1:= true;
  b2:= true;
  b3:= true;
END;' LANGUAGE plpgsql;

CREATE FUNCTION f_in_inout_inout(IN b1 BOOLEAN, INOUT b2 BOOLEAN, INOUT b3 BOOLEAN) AS '
BEGIN
  b1:= true;
  b2:= true;
  b3:= true;
END;' LANGUAGE plpgsql;

CREATE FUNCTION f_blank_inout_inout(b1 BOOLEAN, INOUT b2 BOOLEAN, INOUT b3 BOOLEAN) AS '
BEGIN
  b1:= true;
  b2:= true;
  b3:= true;
END;' LANGUAGE plpgsql;

CREATE FUNCTION f_in_out_out(IN b1 BOOLEAN, OUT b2 BOOLEAN, OUT b3 BOOLEAN) AS '
BEGIN
  b2:= b1;
  b3:= true;
END;' LANGUAGE plpgsql;

CREATE FUNCTION f_blank_out_out(b1 BOOLEAN, OUT b2 BOOLEAN, OUT b3 BOOLEAN) AS '
BEGIN
  b2:= b1;
  b3:= true;
END;' LANGUAGE plpgsql;

CREATE TABLE temporal_data_type
(
  "id"          INTEGER NOT NULL PRIMARY KEY,
  "Date"        date,
  "Time"        time without time zone,
  "TimeTz"      time with time zone,
  "Timestamp"   timestamp without time zone,
  "TimestampTz" timestamp with time zone,
  "Interval"    interval
);

--- TemporalDataType
INSERT INTO temporal_data_type ("id", "Date", "Time", "TimeTz", "Timestamp", "TimestampTz", "Interval")
VALUES (1, '1981-05-30', '17:55:04.90512', '17:55:04.90512+03:07', '2017-05-14 19:35:58.237666',
        '2017-05-14 23:59:59.237666-03', '10 years 3 months 332 days 20 hours 20 minutes 20.999999 seconds');
