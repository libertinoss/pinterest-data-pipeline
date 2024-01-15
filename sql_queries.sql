/* 
What is the most popular Pinterest category people post to in each each country?
*/

WITH cte1 AS (
    SELECT
        g.country,
        p.category AS favourite_category,
        COUNT(p.category) AS category_count
    FROM
        df_pin AS p
    JOIN
        df_geo AS g ON p.ind = g.ind
    GROUP BY
        g.country, p.category
),
cte2 AS (
    SELECT
        country,
        MAX(category_count) AS max_category_count
    FROM
        cte1
    GROUP BY
        country
)
SELECT 
    cte1.country,
    cte1.favourite_category,
    cte1.category_count
FROM 
    cte1
JOIN 
    cte2 ON cte1.country = cte2.country
WHERE 
    cte1.category_count = cte2.max_category_count
ORDER BY 
    country

/*
For each year between 2018 and 2022 which was the most popular category?
*/

WITH cte1 AS (
    SELECT
        EXTRACT(YEAR FROM g.timestamp) AS post_year,
        p.category AS favourite_category,
        COUNT(p.category) AS category_count
    FROM
        df_pin AS p
    JOIN
        df_geo AS g ON p.ind = g.ind
    GROUP BY
        EXTRACT(YEAR FROM g.timestamp), p.category
),
cte2 AS (
    SELECT
        post_year,
        MAX(category_count) AS max_category_count
    FROM
        cte1
    GROUP BY
        post_year
)
SELECT 
    cte1.post_year,
    cte1.favourite_category,
    cte1.category_count
FROM 
    cte1
JOIN 
    cte2 ON cte1.post_year = cte2.post_year
WHERE 
    cte1.category_count = cte2.max_category_count AND cte1.post_year BETWEEN 2018 and 2022
ORDER BY 
    post_year

/*
Which user has the most followers in each country?
*/

WITH ctemax AS (
SELECT
    country,
    MAX(follower_count) as follower_count
FROM
    df_geo g
JOIN 
    df_pin p ON g.ind = p.ind
GROUP BY
    country),

ctenormal AS (
SELECT
    country,
    user_name,
    follower_count
FROM
    df_geo g
JOIN 
    df_user u ON u.ind = g.ind
JOIN 
    df_pin p ON p.ind = g.ind)

SELECT DISTINCT
    ctemax.country,
    ctenormal.user_name AS poster_name,
    ctemax.follower_count
FROM 
    ctemax
JOIN 
    ctenormal ON ctemax.follower_count = ctenormal.follower_count AND ctenormal.country = ctemax.country
ORDER BY
    follower_count

/*
Which is the most popular category for people to post based on age group?
*/

-- New column added for age group and age group bins defined
ALTER TABLE df_user 
    ADD COLUMN age_group string;

UPDATE df_user
    SET age_group = CASE
        WHEN age >= 18 AND age <= 24 THEN '18-24'
        WHEN age >= 25 AND age <= 35 THEN '25-30'
        WHEN age >= 36 AND age <= 50 THEN '36-50'
        WHEN age >  50 THEN '50+'
    END;

-- Most popular categories for each age group retrieved using similar subquery structure to previous queries
WITH cte1 AS (
    SELECT
        age_group,
        category AS favourite_category,
        COUNT(category) AS category_count
    FROM
        df_user AS u
    JOIN
        df_pin AS p ON p.ind = u.ind
    GROUP BY
        age_group, category
),
cte2 AS (
    SELECT
        age_group,
        MAX(category_count) AS max_category_count
    FROM
        cte1
    GROUP BY
        age_group
)
SELECT 
    cte1.age_group,
    cte1.favourite_category,
    cte1.category_count
FROM 
    cte1
JOIN 
    cte2 ON cte1.age_group = cte2.age_group
WHERE 
    cte1.category_count = cte2.max_category_count 
ORDER BY
    cte1.age_group

/*
What is the median follower count for users in the different age groups?
*/

SELECT 
    age_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count
FROM 
    (SELECT
        DISTINCT(user_name),
        age_group,
        follower_count
    FROM
        df_pin AS p
    JOIN
        df_user AS u ON u.ind = p.ind)
GROUP BY
    age_group

/*
How many users have joined every year?
*/

SELECT
    EXTRACT(YEAR FROM date_joined) AS join_year,
    COUNT(DISTINCT(user_name)) AS users_joined
FROM
    df_user
GROUP BY
    join_year

/*
What is the median follower count of users based on the year they joined?
*/

SELECT 
    join_year,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count
FROM 
    (SELECT
        DISTINCT(user_name),
        EXTRACT(YEAR FROM date_joined) AS join_year,
        follower_count
    FROM
        df_pin AS p
    JOIN
        df_user AS u ON u.ind = p.ind)
GROUP BY
    join_year
ORDER BY
    join_year

/*
What is the median follower count of users based on their join year, split by agegroup?
*/

SELECT 
    join_year,
    age_group,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY follower_count) AS median_follower_count
FROM 
    (SELECT
        DISTINCT(user_name),
        EXTRACT(YEAR FROM date_joined) AS join_year,
        follower_count,
        age_group
    FROM
        df_pin AS p
    JOIN
        df_user AS u ON u.ind = p.ind)        
GROUP BY
    join_year, age_group
ORDER BY
    join_year, age_group