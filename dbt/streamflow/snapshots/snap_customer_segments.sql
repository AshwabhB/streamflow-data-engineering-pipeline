{% snapshot snap_customer_segments %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_segment',
            'total_spend',
            'total_transactions',
            'failure_rate_pct',
        ],
    )
}}

select
    customer_id,
    customer_name,
    customer_email,
    customer_country,
    total_spend,
    total_transactions,
    avg_transaction_value,
    active_days,
    first_seen_at,
    last_seen_at,
    categories_purchased,
    failed_transactions,
    failure_rate_pct,
    customer_segment,
    dbt_updated_at

from {{ ref('dim_customers') }}

{% endsnapshot %}
