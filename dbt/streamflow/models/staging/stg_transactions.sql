{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        customer_id,
        lower(trim(customer_name))              as customer_name,
        lower(trim(customer_email))             as customer_email,
        upper(trim(customer_country))           as customer_country,
        cast(amount as numeric)                 as amount,
        upper(trim(currency))                   as currency,
        lower(trim(status))                     as status,
        lower(trim(product_category))           as product_category,
        trim(product_name)                      as product_name,
        cast(quantity as int64)                 as quantity,
        timestamp(timestamp)                    as event_timestamp,
        ip_address,
        lower(trim(device_type))                as device_type,
        lower(trim(payment_method))             as payment_method,
        date(timestamp(timestamp))              as transaction_date,
        extract(hour from timestamp(timestamp)) as transaction_hour

    from source

    where transaction_id is not null
      and customer_id is not null
      and amount > 0
      and amount < 1000000
      and status in ('success', 'failed', 'pending', 'refunded')
)

select * from cleaned
