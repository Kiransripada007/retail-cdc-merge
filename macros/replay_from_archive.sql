{% macro replay_from_archive(queue_table_name, hook_type, database='RDL_TMS_SVT', schema='CDC') %}
    {% if var('is_replay', false) %}

        {% set queue_fqn      = database ~ '.' ~ schema ~ '.' ~ queue_table_name %}
        {% set replay_date    = var('replay_watermark') | replace('-','') | replace(' ','') | replace(':','') %}
        {% set staging_fqn    = database ~ '.CA.' ~ queue_table_name ~ '_REPLAY_' ~ replay_date[:8] %}

        {% if hook_type == 'pre' %}

            {{ log("replay_from_archive [pre-hook]: detaching SLP from " ~ queue_fqn, info=True) }}
            {{ log("Replay window: " ~ var('replay_watermark') ~ " → " ~ var('replay_end'), info=True) }}
            {{ detach_slp(queue_table_name, database=database, schema=schema) }}
            {% set create_staging %}
                create or replace transient table {{ staging_fqn }} as
                select *
                from table(
                    {{ queue_fqn }}!restore_archived_data(
                        archive_timestamp_range => (
                            '{{ var("replay_watermark") }}'::timestamp_ntz,
                            '{{ var("replay_end") }}'::timestamp_ntz
                        )
                    )
                )
            {% endset %}

            {{ log("REPLAY: Creating transient staging table " ~ staging_fqn, info=True) }}
            {% do run_query(create_staging) %}
            {{ log("REPLAY: Staging table ready.", info=True) }}

        {% elif hook_type == 'post' %}

            {{ log("replay_from_archive [post-hook]: re-attaching SLP to " ~ queue_fqn, info=True) }}
            {{ attach_slp(queue_table_name, database=database, schema=schema) }}
            {% set drop_staging %}
                drop table if exists {{ staging_fqn }}
            {% endset %}
            {% do run_query(drop_staging) %}
            {{ log("REPLAY: Staging table " ~ staging_fqn ~ " dropped.", info=True) }}

        {% else %}

            {{ exceptions.raise_compiler_error(
                "replay_from_archive: hook_type must be 'pre' or 'post', got '" ~ hook_type ~ "'"
            ) }}

        {% endif %}

    {% endif %}

{% endmacro %}