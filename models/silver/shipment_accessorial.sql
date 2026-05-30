{{
    config(
        materialized='incremental',
        unique_key=['SHIPMENT_ID','SHP_ACCESSORIAL_ID','RESOURCE_TYPE'],
        on_schema_change='fail'
    )
}}

{{
    cdc_merge(
        source_relation=source('bronze','shipment_accessorial_queue'),
        target_relation=this,
        primary_key=['SHIPMENT_ID','SHP_ACCESSORIAL_ID','RESOURCE_TYPE']
    )
}}