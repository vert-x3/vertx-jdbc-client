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
