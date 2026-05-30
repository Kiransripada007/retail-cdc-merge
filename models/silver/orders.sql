{{
    config(
        materialized='incremental',
        unique_key=['ORDER_ID'],
        on_schema_change='fail'
    )
}}

{{
    cdc_merge(
        source_relation=source('bronze','orders_queue'),
        target_relation=this,
        primary_key=['ORDER_ID']
    )
}}