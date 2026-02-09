SELECT
    id,
    name,
    country,
    country='Nepal' AS is_local
FROM
    {{ ref('customers') }}

