-- Calculate the money lost due to discounts. Use lineitem to get the price of items (without discounts) that are part of an order and compare it to the order.
%%sql
WITH lineitem_agg AS (
    SELECT 
        l_orderkey,
        SUM(l_extendedprice) AS total_price_without_discount
    FROM 
        lineitem
    GROUP BY 
        l_orderkey
)
SELECT 
    o.o_orderkey,
    o.o_totalprice, 
    l.total_price_without_discount - o.o_totalprice AS amount_lost_to_discount
FROM 
    orders o
JOIN 
    lineitem_agg l ON o.o_orderkey = l.l_orderkey
ORDER BY 
    o.o_orderkey;