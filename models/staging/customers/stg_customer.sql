with source as (

    select * from {{ source('raw', 'customers') }}

),

renamed as (

    select
        customer_id,
        lower(trim(first_name)) as first_name,
        lower(trim(last_name)) as last_name,
        lower(trim(email)) as email,
        cast(signup_date as date) as signup_date
    from source

)

select * from renamed



