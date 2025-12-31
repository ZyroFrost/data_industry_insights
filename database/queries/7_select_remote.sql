WITH t_remote AS 
  (SELECT
    extract(year from posted_date) as year,
    remote_option
  FROM data_industry_insights.job_postings
  WHERE remote_option IS NOT NULL)
  
SELECT year, count(*)
FROM t_remote
GROUP BY year