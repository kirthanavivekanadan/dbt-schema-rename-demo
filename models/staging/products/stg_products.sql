with source as (

    select * from {{ source('raw', 'products') }}

),

renamed as (

    select
        product_id,
        lower(product_name) as product_name,
        lower(category) as category,
        cast(price as float) as price
    from source

)

select * from renamed
