"""
SQL queries for the StreamFlow dashboard.
All queries are functions that accept the fully-qualified dataset prefix
(e.g. 'my-project.streamflow_marts') so they compose cleanly.
"""


def kpis_today(marts: str) -> str:
    return f"""
        SELECT
            COALESCE(SUM(total_revenue), 0)               AS revenue_today,
            COALESCE(SUM(order_count), 0)                 AS orders_today,
            COALESCE(ROUND(AVG(avg_order_value), 2), 0)   AS avg_order_value,
            COALESCE(ROUND(AVG(failure_rate_pct), 2), 0)  AS avg_failure_rate
        FROM `{marts}.fct_daily_revenue`
        WHERE transaction_date = CURRENT_DATE()
    """


def revenue_trend(marts: str, days: int) -> str:
    return f"""
        SELECT
            transaction_date,
            product_category,
            SUM(total_revenue) AS daily_revenue,
            SUM(order_count)   AS daily_orders
        FROM `{marts}.fct_daily_revenue`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY transaction_date, product_category
        ORDER BY transaction_date
    """


def category_breakdown(marts: str, days: int) -> str:
    return f"""
        SELECT
            product_category,
            SUM(total_revenue)                                                  AS revenue,
            SUM(order_count)                                                    AS orders,
            ROUND(SAFE_DIVIDE(SUM(failed_orders), SUM(order_count)) * 100, 2)  AS failure_rate_pct,
            ROUND(SAFE_DIVIDE(SUM(total_revenue), SUM(order_count)), 2)         AS avg_order_value
        FROM `{marts}.fct_daily_revenue`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY product_category
        ORDER BY revenue DESC
    """


def top_customers(marts: str, limit: int = 20) -> str:
    return f"""
        SELECT
            customer_id,
            customer_name,
            customer_country,
            customer_segment,
            ROUND(total_spend, 2)           AS total_spend,
            total_transactions,
            ROUND(avg_transaction_value, 2) AS avg_transaction_value,
            failure_rate_pct,
            active_days
        FROM `{marts}.dim_customers`
        ORDER BY total_spend DESC
        LIMIT {limit}
    """


def payment_method_failure(marts: str) -> str:
    return f"""
        SELECT
            payment_method,
            COUNT(*)                                                              AS total_orders,
            COUNTIF(status = 'failed')                                            AS failed_orders,
            ROUND(SAFE_DIVIDE(COUNTIF(status = 'failed'), COUNT(*)) * 100, 2)    AS failure_rate_pct,
            ROUND(SUM(amount), 2)                                                 AS total_revenue,
            ROUND(AVG(amount), 2)                                                 AS avg_order_value
        FROM `{marts}.fct_transactions`
        WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY payment_method
        ORDER BY failure_rate_pct DESC
    """


def customer_segments(marts: str) -> str:
    return f"""
        SELECT
            customer_segment,
            COUNT(*)                    AS customer_count,
            ROUND(SUM(total_spend), 2)  AS segment_revenue,
            ROUND(AVG(total_spend), 2)  AS avg_spend
        FROM `{marts}.dim_customers`
        GROUP BY customer_segment
        ORDER BY segment_revenue DESC
    """
