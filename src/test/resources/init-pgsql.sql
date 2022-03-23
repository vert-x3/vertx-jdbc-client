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
