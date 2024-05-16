-- Query 1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
SELECT FORMAT_DATETIME('%b %Y', s.modifieddate) AS period,
      ps.name AS name,
      SUM(OrderQty) qty_item,
      SUM(LineTotal) AS total_sales,
      COUNT(DISTINCT SalesOrderID) AS order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` AS s
LEFT JOIN `adventureworks2019.Production.Product` AS pp
  USING(productid)
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS ps
  ON CAST(pp.ProductSubcategoryID AS DECIMAL) = ps.ProductSubcategoryID
GROUP BY period, name
ORDER BY period DESC, name;

-- Query 2: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. Can use metric: quantity_item. Round results to 2 decimal
WITH subcate AS (
  SELECT FORMAT_DATETIME('%Y', s.modifieddate) AS year,
        ps.name AS name,
        SUM(OrderQty) AS qty_item,
  FROM `adventureworks2019.Sales.SalesOrderDetail` AS s
  LEFT JOIN `adventureworks2019.Production.Product` AS pp 
    USING(productid)
  LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS ps
    ON CAST(pp.ProductSubcategoryID AS DECIMAL) = ps.ProductSubcategoryID
  GROUP BY year, name
),
sales_diff AS (
  SELECT name, qty_item,
        LEAD(qty_item) OVER(PARTITION BY name ORDER BY year DESC) AS prev_qty,
        ROUND(qty_item/ (LEAD(qty_item) OVER(PARTITION BY name ORDER BY year DESC)) - 1, 2) AS qty_diff
  FROM subcate
),
ranking AS (
 SELECT *,
      DENSE_RANK() OVER(ORDER BY qty_diff DESC) AS dr    
 FROM sales_diff
)
SELECT name, qty_item, prev_qty, qty_diff
FROM ranking
WHERE dr <= 3
ORDER BY dr;

-- Query 3: Ranking Top 3 TeritoryID with biggest Order quantity of every year. If there's TerritoryID with same quantity in a year, do not skip the rank number
WITH convert_year AS (
      SELECT FORMAT_DATETIME('%Y', s.modifieddate) AS year, 
            TerritoryID,
            SUM(OrderQty) AS order_cnt
      FROM `adventureworks2019.Sales.SalesOrderDetail` AS s
      LEFT JOIN adventureworks2019.Sales.SalesOrderHeader AS sh USING(salesorderid)
      GROUP BY year, TerritoryID
),
     ranking AS ( 
      SELECT year,
            TerritoryID,
            order_cnt,
            DENSE_RANK() OVER(PARTITION BY year ORDER BY order_cnt DESC) AS rk
      FROM convert_year
)
SELECT year,
      TerritoryID,
      order_cnt,
      rk
FROM ranking
WHERE rk<= 3
ORDER BY year DESC;

-- Query 4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
SELECT FORMAT_DATETIME('%Y', s.modifieddate) AS year,
    ps.name AS name,
    SUM(DiscountPct * UnitPrice * OrderQty) AS total_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` AS s
LEFT JOIN `adventureworks2019.Production.Product` AS pp
  USING(productid)
LEFT JOIN `adventureworks2019.Production.ProductSubcategory` AS ps
  ON CAST(pp.ProductSubcategoryID AS DECIMAL) = ps.ProductSubcategoryID
LEFT JOIN `adventureworks2019.Sales.SpecialOffer` AS ss 
  ON s.SpecialOfferID = ss.SpecialOfferID
WHERE type LIKE '%Seasonal Discount%'
GROUP BY year, name;

-- Query 5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
WITH general_info AS (
  SELECT EXTRACT(MONTH FROM modifieddate) AS month,
        EXTRACT(YEAR FROM modifieddate) AS year,
        CustomerID,
        COUNT(DISTINCT SalesOrderID) AS sales_cnt
  FROM `adventureworks2019.Sales.SalesOrderHeader`
  WHERE status = 5 AND EXTRACT(YEAR FROM modifieddate) = 2014
  GROUP BY 1, 2, 3
),
    row_num AS (
  SELECT month,
        CustomerID,
        ROW_NUMBER() OVER(PARTITION BY customerid ORDER BY month) AS rn
  FROM general_info
),
    first_order AS (
  SELECT month AS month_join,
        CustomerID
  FROM row_num
  WHERE rn = 1
),
    consolidate AS (
  SELECT DISTINCT month, month_join, CustomerID,
        CONCAT('M - ', (month - month_join)) AS month_diff
  FROM general_info AS g
  LEFT JOIN first_order AS f USING(CustomerID)
  ORDER BY CustomerID
)
SELECT month_join, month_diff,
      COUNT(DISTINCT CustomerID) AS customer_cnt
FROM consolidate
GROUP BY month_join, month_diff
ORDER BY month_join, month_diff;

-- Query 6: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
WITH stock_2011 AS (
  SELECT 
        name,
        EXTRACT(MONTH FROM pw.modifieddate) AS month,
        EXTRACT(YEAR FROM pw.modifieddate) AS year,
        SUM(StockedQty) stock_qty
  FROM `adventureworks2019.Production.Product` AS pp
  LEFT JOIN `adventureworks2019.Production.WorkOrder` AS pw 
    USING(productid)
  WHERE EXTRACT(YEAR FROM pw.modifieddate) = 2011
  GROUP BY name, month, year
),
    prev_month AS (
  SELECT name, month, year, stock_qty,
        LAG(stock_qty) OVER(PARTITION BY name ORDER BY month) AS stock_prev
  FROM stock_2011
)
SELECT name, month, year, stock_qty, stock_prev,
      CASE 
        WHEN stock_prev IS NULL THEN 0
        ELSE ROUND((stock_qty - stock_prev) * 100/ stock_prev, 1) END AS diff
FROM prev_month
ORDER BY name, month DESC;

-- Query 7: Calc Ratio of Stock / Sales in 2011 by product name, by month. Order results by month desc, ratio desc. Round Ratio to 1 decimal mom yoy
WITH sales_info AS (
      SELECT EXTRACT(MONTH FROM so.modifieddate) AS month,
            EXTRACT(YEAR FROM so.modifieddate) AS year,
            so.productid,
            name,
            SUM(orderqty) AS sales
      FROM `adventureworks2019.Sales.SalesOrderDetail` AS so
      LEFT JOIN `adventureworks2019.Production.Product` AS pp
            USING(productid)
      WHERE EXTRACT(YEAR FROM so.modifieddate) = 2011
      GROUP BY month, year, so.productid, name
 ),
stock_info AS (
      SELECT EXTRACT(MONTH FROM modifieddate) AS month,
            EXTRACT(YEAR FROM modifieddate) AS year,
            productid,
            SUM(stockedqty) AS stock
      FROM `adventureworks2019.Production.WorkOrder`
      WHERE EXTRACT(YEAR FROM modifieddate) = 2011
      GROUP BY month, year, productid
)
SELECT s2.month, s2.year, s2.productid, name, sales, stock,
      ROUND((stock/ sales),1) AS ratio
FROM sales_info AS s1
LEFT JOIN stock_info AS s2 ON s1.productid = s2.productid
      AND s1.month = s2.month AND s1.year = s2.year
ORDER BY month DESC, ratio DESC;

-- Query 8: No of order and value at Pending status in 2014
SELECT EXTRACT(YEAR FROM modifieddate) AS year,
      status,
      COUNT(DISTINCT purchaseorderid) AS order_cnt,
      SUM(totaldue) AS value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE EXTRACT(YEAR FROM modifieddate) = 2014 AND status = 1
GROUP BY year, status;