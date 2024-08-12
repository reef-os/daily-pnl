SELECT
    "item" AS category,
    "vessel id" AS vessel,
    "amount" AS amount_local,
    "percentage" AS percentage,
    "start date" AS start_date,
    "end date" AS end_date,
    "functionality" AS  functionality,
    "type" AS type,
    "statement type" AS statement_type,
    uc.country AS country,
    uc."vessel name" AS vessel_name
FROM monday.statement_exceptions
LEFT JOIN monday.ulysses_crm uc ON uc."vessel code" = "vessel id"
WHERE uc.country = 'US'
AND uc.stage = 'Active' and end_date is NULL