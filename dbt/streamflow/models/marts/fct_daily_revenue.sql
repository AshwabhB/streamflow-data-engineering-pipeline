{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["product_category"]
    )
}}

with transactions as (
    select * from {{ ref('fct_transactions') }}
),

daily_revenue as (
    select
        transaction_date,
        product_category,

        count(*) as order_count,
        count(case when status = 'success' then 1 end) as successful_orders,
        count(case when status = 'failed' then 1 end) as failed_orders,
        count(case when status = 'refunded' then 1 end) as refunded_orders,
        count(case when status = 'pending' then 1 end) as pending_orders,
        count(case when is_high_value then 1 end) as high_value_orders,

        round(sum(amount), 2) as total_revenue,
        round(
            sum(case when status = 'success' then amount else 0 end),
            2
        ) as successful_revenue,
        round(avg(amount), 2) as avg_order_value,
        sum(quantity) as total_quantity_sold,

        round(
            safe_divide(
                count(case when status = 'failed' then 1 end),
                count(*)
            ) * 100,
            2
        ) as failure_rate_pct,

        current_timestamp() as dbt_updated_at

    from transactions
    group by
        transaction_date,
        product_category
)

select * from daily_revenue
