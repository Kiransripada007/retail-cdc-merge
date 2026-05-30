{% macro cdc_merge(
    source_relation,
    target_relation,
    primary_key,
    is_replay='false'
)%}

{% set cdc_enriched_query %}
    with cdc_data as (
        select * from {{source_relation}} {% if is_incremental() and not var('is_replay',false) %}
            where load_timestamp_utc >= (select coalesce(max(load_timestamp_utc),'1900-01-01') from {{target_relation}})
            {% elif var('is_replay',false) %}
                where load_timestamp_utc >= (
                    select least(coalesce(max(load_timestamp_utc),'1900-01-01'::timestamp_ntz),'{{var("replay_watermark")}}'::timestamp_ntz)
                    from {{target_relation}}
                )
                and load_timestamp_utc <= '{{var("replay_end")}}'::timestamp_ntz
            {% endif %}
    ),

    deduplicated as (
        select * from cdc_data
        qualify row_number() over (partition by {{primary_key | join(', ')}} 
        order by load_cdc_scn desc, load_cdc_sequence_internal desc
        )=1
    ),

    archive_logic as (
        select *, max(CASE when upper(trim(data_source)) = 'CA22' and upper(trim(load_operation)) = 'D' then 1 else 0 end
        ) over (partition by {{primary_key | join(', ')}}) as has_active_delete,
        max(CASE when upper(trim(data_source))='ARCHCA22' then 1 else 0 end
        ) over (partition by {{primary_key | join(', ')}}) as has_archive_record
        from deduplicated
    ),

    final_source as (
        select *,
            case 
                when has_active_delete =1 and has_archive_record=1 then 'U' else load_operation
            end as final_load_operation,
            case
                when has_active_delete=1 and has_archive_record=1 then 'ARCHCA22' else data_source
            end as final_data_source
        from archive_logic
    )

    select * from final_source
    {% endset %}

    merge into {{target_relation}} as tgt using ({{cdc_enriched_query}}) as src on
    {% for col in primary_key %}
    src.{{col}} = tgt.{{col}}{% if not loop.last %} and {% endif %}
    {% endfor %}

    {% if var('is_replay', false) %}
        and (
            src.load_cdc_scn > tgt.load_cdc_scn
            or (
                src.load_cdc_scn =tgt.load_cdc_scn and src.load_cdc_sequence_internal > tgt.load_cdc_sequence_internal
            )
        )
    {% endif %}

    when matched and src.final_load_operation in ('U','D')
    then update set 
        {% for col in adapter.get_columns_in_relation(source_relation) %}
            {% if col.name not in primary_key %}
                tgt.{{col.name}} = src.{{col.name}}{% if not loop.last %}, {% endif %}
            {% endif %}
        {% endfor %},
        tgt.load_operation = src.final_load_operation,
        tgt.data_source = src.final_data_source
    
    when not matched and src.final_load_operation in ('I','U')
    then insert (
        {% for col in adapter.get_columns_in_relation(source_relation) %}
            {{col.name}}{% if not loop.last %}, {% endif %}
        {% endfor %}
    ) values (
        {% for col in adapter.get_columns_in_relation(source_relation) %}
            src.{{col.name}}{% if not loop.last %}, {% endif %}
        {% endfor %}
    )

{% endmacro %}