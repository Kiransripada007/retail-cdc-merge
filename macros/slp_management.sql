{% macro attach_slp(table_name) %}
    {% set sql %}
        alter table rdl_tms_svt.cdc.{{table_name}}
        add storage lifecycle policy rdl_tms_svt.cdc.cdc_queue_slp on (load_timestamp_utc);
    {% endset %}

    {{ log("Attaching SLP to " ~ table_name, info=True)}}
    {% do run_query(sql) %}
    {{log("SLP attachment complete.",info=True)}}
{% endmacro %}

{% macro detach_slp(table_name) %}
    {% set sql %}
        alter table rdl_tms_svt.cdc.{{table_name}}
        remove storage lifecycle policy;
    {% endset %}

    {{ log("Detaching SLP from " ~ table_name, info=True)}}
    {% do run_query(sql) %}
    {{log("SLP detachment complete.",info=True)}}
{% endmacro %}