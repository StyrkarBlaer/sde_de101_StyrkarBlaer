-- Windows

-- Write a query to calculate the daily running average of totalprice of every customer. 
%%sql
SELECT
    o_custkey,
    o_orderdate,
    o_totalprice,
    avg(o_totalprice)
    OVER (
        PARTITION BY
            o_custkey
        ORDER BY
            o_orderdate
    ) AS 'Average Sum'
FROM
    orders
ORDER BY
    o_custkey
LIMIT
    10;


-- From the `orders` table get the 3 lowest spending customers per day
-- i'm pretty sure this answer is wrong, but the given answer is this, so idk lol 
%%sql
SELECT
  *
FROM
  (
    SELECT
      o_orderdate,
      o_totalprice,
      o_custkey,
      RANK()
      OVER (
        PARTITION BY
          o_orderdate -- PARTITION BY order date
        ORDER BY
          o_totalprice DESC -- ORDER rows withing partition by totalprice
      ) AS rnk
    FROM
      orders
  )
ORDER BY
  rnk <= 3
LIMIT 
  5;


-- Write a SQL query using the `orders` table that calculates the following columns
-- 1. o_orderdate: From orders table
-- 2. o_custkey: From orders table
-- 3. o_totalprice: From orders table
-- 4. totalprice_diff: The customers current day's o_totalprice - that same customers most recent previous purchase's o_totalprice
%%sql
SELECT
    o_orderdate,
    o_custkey,
    o_totalprice,
    o_totalprice - LAG(o_totalprice, 1) OVER (
        PARTITION BY
            o_custkey
        ORDER BY
            o_orderdate
    ) AS totalprice_diff
FROM
    orders
ORDER BY
    o_custkey,
    o_orderdate
LIMIT
50;

-- 1. Write a query on the orders table that has the following output:
--  1. order_month, 
--  2. o_custkey,
--  3. total_price,
--  4. three_mo_total_price_avg
--  5. **consecutive_three_mo_total_price_avg**: The consecutive 3 month average of total_price for that customer. Note that this should only include months that are chronologically next to each other.
-- This answer was given.
%%sql
SELECT
  order_month,
  o_custkey,
  total_price,
  ROUND(
    AVG(total_price) OVER (
      PARTITION BY
        o_custkey
      ORDER BY
        CAST(order_month AS DATE) RANGE BETWEEN INTERVAL '1' MONTH PRECEDING
        AND INTERVAL '1' MONTH FOLLOWING
    ),
    2
  ) AS consecutive_three_mo_total_price_avg,
  ROUND(
    AVG(total_price) OVER (
      PARTITION BY
        o_custkey
      ORDER BY
        order_month ROWS BETWEEN 1 PRECEDING
        AND 1 FOLLOWING
    ),
    2
  ) AS three_mo_total_price_avg
FROM
  (
    SELECT
      strftime (o_orderdate, '%Y-%m-01') AS order_month,
      o_custkey,
      SUM(o_totalprice) AS total_price
    FROM
      orders
    GROUP BY
      1,
      2
  )
ORDER BY
  o_custkey,
  order_month
LIMIT
  50;