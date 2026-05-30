{{
    config(
        materialized='incremental',
        unique_key=['ORDER_ID'],
        on_schema_change='fail',

        post_hook="{{ cdc_merge(
            source('bronze','orders_queue'),
            this,
            ['ORDER_ID']
        ) }}"
    )
}}

select *
from {{ source('bronze','orders_queue') }}
where 1 = 0