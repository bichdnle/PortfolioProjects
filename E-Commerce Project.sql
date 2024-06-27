-- Query 1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 (order by month)
SELECT 
      FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      SUM(totals.visits) AS visits,
      SUM(totals.pageviews) AS pageviews,
      SUM(totals.transactions) AS transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;

-- Query 2: Bounce rate per traffic source in July 2017 (Bounce_rate = num_bounce/total_visit) (order by total_visit DESC)
SELECT trafficsource.source AS source,
    SUM(totals.visits) total_visits,
    SUM(totals.bounces) total_bounces,
    SUM(totals.bounces) * 100/ SUM(totals.visits) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
WITH union_time AS(
  SELECT 'Month' AS type_time,
        FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) AS time,
        trafficsource.source AS source,
        SUM(product.productrevenue)/ 1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE product.productrevenue IS NOT NULL
  GROUP BY type_time, time, source

  UNION ALL

  SELECT 'Week' AS type_time,
        FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS week,
        trafficsource.source AS source,
        SUM(product.productrevenue)/ 1000000 AS revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE product.productrevenue IS NOT NULL
  GROUP BY type_time, week, source
  ORDER BY revenue DESC
)
SELECT type_time,
      time,
      source,
      revenue
FROM union_time;

-- Query 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017
WITH purchaser AS(
  SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
        SUM(totals.pageviews)/ COUNT(DISTINCT fullvisitorid) AS avg_pageviews_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
    AND product.productrevenue IS NOT NULL
  GROUP BY month
),
    non_purchaser AS(
  SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
        SUM(totals.pageviews)/ COUNT(DISTINCT fullvisitorid) AS avg_pageviews_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0601' AND '0731'
    AND totals.transactions IS NULL
  GROUP BY month
)
SELECT p.month, avg_pageviews_purchase, avg_pageviews_non_purchase
FROM purchaser AS p
FULL JOIN non_purchaser AS n ON p.month = n.month
ORDER BY p.month;

-- Query 5: Average number of transactions per user that made a purchase in July 2017
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      SUM(totals.transactions)/ COUNT(DISTINCT fullvisitorid) AS avg_transaction_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE product.productrevenue IS NOT NULL
GROUP BY month;

-- Query 6: Average amount of money spent per session. Only include purchaser data in July 2017
SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      SUM(product.productrevenue)/ SUM(totals.visits)/ 1000000 AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
WHERE product.productrevenue IS NOT NULL
GROUP BY month;

-- Query 7: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered
WITH appointed_product AS(
  SELECT DISTINCT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE product.v2productname = "YouTube Men's Vintage Henley"
    AND totals.transactions >= 1
    AND product.productrevenue IS NOT NULL
)
SELECT product.v2productname AS other_purchased_products,
      SUM(product.productquantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) hits,
UNNEST(hits.product) product
JOIN appointed_product USING(fullVisitorId)
WHERE product.v2productname <> "YouTube Men's Vintage Henley"
  AND product.productrevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC;

-- Query 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. For example, 100% product view then 40% add_to_cart and 10% purchase
WITH product_view AS(
  SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      COUNT(product.productSKU) AS num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '2'
  GROUP BY month
),
    addtocart AS(
  SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      COUNT(product.productSKU) AS num_addtocart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '3'
  GROUP BY month
),
purchase AS(
  SELECT FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d',date)) AS month,
      COUNT(product.productSKU) AS num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) hits,
  UNNEST(hits.product) product
  WHERE _table_suffix BETWEEN '0101' AND '0331'
    AND eCommerceAction.action_type = '6'
    AND product.productrevenue IS NOT NULL
  GROUP BY month
)
SELECT product_view.month AS month,
      product_view.num_product_view AS num_product_view,
      addtocart.num_addtocart AS num_addtocart,
      purchase.num_purchase AS num_purchase,
      ROUND((num_addtocart * 100/ num_product_view),2) AS addtocart_rate,
      ROUND((num_purchase * 100/ num_product_view),2) AS purchase_rate
FROM product_view
LEFT JOIN addtocart USING(month)
LEFT JOIN purchase USING(month)
ORDER BY month;
