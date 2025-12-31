WITH t_salary AS 
  (SELECT
    extract(year from posted_date) as year,
    min_salary, max_salary
  FROM job_postings
  WHERE min_salary IS NOT NULL and max_salary IS NOT NULL)
  
SELECT year, count(*) as jobs_count
FROM t_salary
GROUP BY year
