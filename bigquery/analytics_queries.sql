-- ============================================================
-- StreamFlow — Analytics Queries
-- Replace {project} with your GCP project ID before running.
-- Run these in the BigQuery console after dbt completes.
-- ============================================================


-- 1. Total revenue today (successful transactions only)
SELECT
  DATE(event_timestamp)   AS date,
  SUM(amount)             AS total_revenue,
  COUNT(*)                AS transaction_count,
  AVG(amount)             AS avg_transaction_value
FROM `{project}.streamflow_marts.fct_transactions`
WHERE status = 'success'
  AND transaction_date = CURRENT_DATE()
GROUP BY 1;


-- 2. Revenue by product category — last 7 days
SELECT
  product_category,
  SUM(amount)                         AS total_revenue,
  COUNT(*)                            AS transaction_count,
  AVG(amount)                         AS avg_order_value,
  COUNTIF(status = 'failed')          AS failed_transactions
FROM `{project}.streamflow_marts.fct_transactions`
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY product_category
ORDER BY total_revenue DESC;


-- 3. Top 10 customers by lifetime spend
SELECT
  customer_id,
  customer_name,
  customer_country,
  customer_segment,
  total_spend,
  total_transactions,
  avg_transaction_value
FROM `{project}.streamflow_marts.dim_customers`
ORDER BY total_spend DESC
LIMIT 10;


-- 4. Hourly transaction volume — today
SELECT
  transaction_hour,
  COUNT(*)                            AS transactions,
  SUM(amount)                         AS revenue,
  COUNTIF(status = 'success')         AS successful,
  COUNTIF(status = 'failed')          AS failed
FROM `{project}.streamflow_marts.fct_transactions`
WHERE transaction_date = CURRENT_DATE()
GROUP BY transaction_hour
ORDER BY transaction_hour;


-- 5. Failed transaction rate by payment method — last 30 days
SELECT
  payment_method,
  COUNT(*)                                                AS total,
  COUNTIF(status = 'failed')                              AS failed,
  ROUND(COUNTIF(status = 'failed') / COUNT(*) * 100, 2)  AS failure_rate_pct
FROM `{project}.streamflow_marts.fct_transactions`
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY payment_method
ORDER BY failure_rate_pct DESC;


-- 6. High-value & flagged transactions (fraud candidates)
SELECT
  transaction_id,
  customer_id,
  customer_name,
  amount,
  product_category,
  device_type,
  ip_address,
  event_timestamp
FROM `{project}.streamflow_marts.fct_transactions`
WHERE is_high_value = TRUE
   OR is_potential_fraud = TRUE
ORDER BY amount DESC
LIMIT 50;


-- 7. Customer segment distribution
SELECT
  customer_segment,
  COUNT(*)            AS customer_count,
  SUM(total_spend)    AS segment_revenue,
  AVG(total_spend)    AS avg_spend_per_customer
FROM `{project}.streamflow_marts.dim_customers`
GROUP BY customer_segment
ORDER BY segment_revenue DESC;


-- 8. Daily revenue trend — last 30 days
SELECT
  transaction_date,
  SUM(IF(status = 'success', amount, 0))  AS daily_revenue,
  COUNT(*)                                AS total_transactions,
  COUNT(DISTINCT customer_id)             AS unique_customers
FROM `{project}.streamflow_marts.fct_transactions`
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY transaction_date
ORDER BY transaction_date;
