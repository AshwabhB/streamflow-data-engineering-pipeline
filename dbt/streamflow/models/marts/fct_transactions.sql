{{
    config(
        materialized='table',
        partition_by={
            "field": "transaction_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["product_category", "status"]
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

final as (
    select
        t.transaction_id,
        t.customer_id,
        c.customer_name,
        c.customer_email,
        c.customer_country,
        c.customer_segment,

        t.amount,
        t.currency,
        t.status,
        t.product_category,
        t.product_name,
        t.quantity,
        round(t.amount * t.quantity, 2) as total_line_value,

        t.device_type,
        t.payment_method,
        t.ip_address,
        t.transaction_date,
        t.transaction_hour,
        t.event_timestamp,

        case when t.amount > 10000 then true else false end as is_high_value,
        case
            when t.status = 'failed' and c.failure_rate_pct > 20
                then true
            else false
        end as is_potential_fraud,

        current_timestamp() as dbt_updated_at

    from transactions as t
    left join customers as c
        on t.customer_id = c.customer_id
)

select * from final
