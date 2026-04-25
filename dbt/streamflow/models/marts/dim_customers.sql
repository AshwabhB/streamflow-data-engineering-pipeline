{{ config(materialized='table') }}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customer_metrics as (
    select
        customer_id,
        any_value(customer_name) as customer_name,
        any_value(customer_email) as customer_email,
        any_value(customer_country) as customer_country,

        count(distinct transaction_id) as total_transactions,
        sum(if(status = 'success', amount, 0)) as total_spend,
        avg(if(status = 'success', amount, null)) as avg_transaction_value,
        count(distinct transaction_date) as active_days,
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at,
        count(distinct product_category) as categories_purchased,

        countif(status = 'failed') as failed_transactions,
        round(
            safe_divide(countif(status = 'failed'), count(*)) * 100,
            2
        ) as failure_rate_pct

    from transactions
    group by customer_id
)

select
    customer_id,
    customer_name,
    customer_email,
    customer_country,
    total_transactions,
    round(total_spend, 2) as total_spend,
    round(avg_transaction_value, 2) as avg_transaction_value,
    active_days,
    first_seen_at,
    last_seen_at,
    categories_purchased,
    failed_transactions,
    failure_rate_pct,

    case
        when total_spend >= 10000 then 'VIP'
        when total_spend >= 1000 then 'Regular'
        else 'Casual'
    end as customer_segment,

    current_timestamp() as dbt_updated_at

from customer_metrics
