CREATE FUNCTION animal_stats(is_pet BOOLEAN, OUT count BIGINT, OUT perc REAL) AS $$
BEGIN
  count:= (SELECT count(*)
           FROM ANIMAL);
  perc:= (SELECT 100 * CAST(count(*) AS REAL) / count
          FROM ANIMAL
          WHERE ANIMAL.IS_PET = animal_stats.is_pet);
END;
$$ LANGUAGE plpgsql;
