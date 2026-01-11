CREATE OR REPLACE VIEW Dim_Countries AS
SELECT DISTINCT
    country,
    country_iso,
    LOWER(country_iso) AS country_iso_lower,
    'https://flagcdn.com/w40/' || LOWER(country_iso) || '.png' AS flag_url
FROM locations
WHERE country_iso IS NOT NULL;