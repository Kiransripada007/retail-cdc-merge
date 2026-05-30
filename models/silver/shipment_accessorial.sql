{{
    config(
        materialized='incremental',
        unique_key=['SHIPMENT_ID','SHP_ACCESSORIAL_ID','RESOURCE_TYPE'],
        on_schema_change='fail',

        pre_hook="{{ replay_from_archive(
            queue_table_name='shipment_accessorial_queue',
            hook_type='pre'
        ) }}",

        post_hook=[
            "{{ cdc_merge(
                source('bronze','shipment_accessorial_queue'),
                this,
                ['SHIPMENT_ID','SHP_ACCESSORIAL_ID','RESOURCE_TYPE']
            ) }}",
            "{{ replay_from_archive(
                queue_table_name='shipment_accessorial_queue',
                hook_type='post'
            ) }}"
        ]
    )
}}

-- Schema bootstrap: on first run (full-refresh) this SELECT defines the table
-- structure by mirroring the queue schema. No rows are inserted — cdc_merge
-- in the post_hook handles all data movement on every run.
select *
from {{ source('bronze','shipment_accessorial_queue') }}
where 1 = 0