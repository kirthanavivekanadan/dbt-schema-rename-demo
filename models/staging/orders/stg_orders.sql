with source as (

    select * from {{ source('raw', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        product_id,
        cast(order_date as date) as order_date,
        quantity
    from source

)

select * from renamed
