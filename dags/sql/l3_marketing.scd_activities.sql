--
-- GENERATED CODE. DON'T MODIFY THIS FILE !!!
--
-- INCREMENTAL
MERGE l3_marketing.scd_activities o
USING (
  WITH src AS (
    SELECT * FROM (
      SELECT *
      FROM activities
      WHERE run_date = @end_date
    )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _id ORDER BY updated_at_wib DESC) = 1
  ),
  trgt AS (
    SELECT * from l3_marketing.scd_activities
    WHERE dim_end_datetime IS NULL
  ),
  delta AS (
    SELECT src.* 
    FROM src
    INNER JOIN trgt
      ON src._id = trgt._id
      AND src.updated_at_wib > trgt.updated_at_wib
  )
  SELECT 
    updated_at_wib AS dim_start_datetime
    , DATETIME(NULL) AS dim_end_datetime
    , dim_is_active
    , * EXCEPT(dim_is_active)
  FROM (
    SELECT *, FALSE AS dim_is_active
    FROM delta
    UNION ALL
    SELECT *, TRUE AS dim_is_active
    FROM delta
    UNION ALL
    SELECT *, TRUE AS dim_is_active
    FROM src
    WHERE src._id NOT IN (
      SELECT _id from trgt
    )
  )
) n
  ON o.dim_end_datetime IS NULL
  AND o._id = n._id 
  AND NOT n.dim_is_active
WHEN MATCHED THEN
  UPDATE SET
    o.dim_end_datetime = n.updated_at_wib
    , o.dim_is_active = FALSE
WHEN NOT MATCHED THEN
  INSERT ROW
;