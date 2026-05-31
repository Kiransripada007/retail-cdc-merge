{% macro cdc_merge(
    source_relation,
    target_relation,
    primary_key,
    live_schema='CA22',
    archive_schema='ARCHCA22'
) %}

{% set pk_upper = primary_key | map('upper') | list %}

{% set cdc_enriched_query %}
    with cdc_data as (
        select *
        from {{ source_relation }}
        {% if is_incremental() and not var('is_replay', false) %}
            where load_timestamp_utc >= (
                select coalesce(
                    max(load_timestamp_utc),
                    '1900-01-01'::timestamp_ntz
                )
                from {{ target_relation }}
            )
        {% elif var('is_replay', false) %}
            where load_timestamp_utc >= (
                select least(
                    coalesce(max(load_timestamp_utc), '1900-01-01'::timestamp_ntz),
                    '{{ var("replay_watermark") }}'::timestamp_ntz
                )
                from {{ target_relation }}
            )
            and load_timestamp_utc <= '{{ var("replay_end") }}'::timestamp_ntz
        {% endif %}
    ),

    archive_logic as (
        select
            *,
            max(
                case
                    when upper(trim(data_source))  = '{{ live_schema }}'
                     and upper(trim(load_operation)) = 'D'
                    then 1 else 0
                end
            ) over (partition by {{ primary_key | join(', ') }}) as has_active_delete,
            max(
                case
                    when upper(trim(data_source)) = '{{ archive_schema }}'
                    then 1 else 0
                end
            ) over (partition by {{ primary_key | join(', ') }}) as has_archive_record,
            case
                when upper(trim(data_source)) = '{{ archive_schema }}'
                then 0 else 1
            end as _archive_priority
        from cdc_data
    ),

    deduplicated as (
        select *
        from archive_logic
        qualify row_number() over (
            partition by {{ primary_key | join(', ') }}
            order by
                _archive_priority asc,
                load_cdc_scn desc,
                load_cdc_sequence_internal desc
        ) = 1
    ),

    final_source as (
        select
            * exclude (_archive_priority),
            case
                when has_active_delete = 1
                 and has_archive_record = 1
                then 'U'
                else load_operation
            end as final_load_operation,
            case
                when has_active_delete = 1
                 and has_archive_record = 1
                then '{{ archive_schema }}'
                else data_source
            end as final_data_source
        from deduplicated
    ),

    {% if var('is_replay', false) %}
    scn_guarded as (
        select src.*
        from final_source src
        left join {{ target_relation }} tgt
            on {% for col in primary_key %}
               src.{{ col }} = tgt.{{ col }}{% if not loop.last %} and {% endif %}
               {% endfor %}
        where tgt.{{ primary_key[0] }} is null
           or src.load_cdc_scn > tgt.load_cdc_scn
           or (
                src.load_cdc_scn = tgt.load_cdc_scn
                and src.load_cdc_sequence_internal > tgt.load_cdc_sequence_internal
              )
    )
    {% else %}
    scn_guarded as (
        select * from final_source
    )
    {% endif %}

    select * from scn_guarded

{% endset %}

-- ── MERGE statement ── ──
merge into {{ target_relation }} as tgt
using ( {{ cdc_enriched_query }} ) as src
on
    {% for col in primary_key %}
    src.{{ col }} = tgt.{{ col }}{% if not loop.last %} and {% endif %}
    {% endfor %}

when matched and src.final_load_operation in ('U', 'D')
then update set
    {% set ns = namespace(first=true) %}
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        {% if col.name | upper not in pk_upper
              and col.name | upper not in [
                  'FINAL_LOAD_OPERATION', 'FINAL_DATA_SOURCE',
                  'HAS_ACTIVE_DELETE', 'HAS_ARCHIVE_RECORD',
                  'LOAD_OPERATION', 'DATA_SOURCE'
              ] %}
            {% if not ns.first %}, {% endif %}
            tgt.{{ col.name }} = src.{{ col.name }}
            {% set ns.first = false %}
        {% endif %}
    {% endfor %}
    , tgt.load_operation = src.final_load_operation
    , tgt.data_source    = src.final_data_source

when not matched and src.final_load_operation in ('I', 'U')
then insert (
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        {{ col.name }}{% if not loop.last %}, {% endif %}
    {% endfor %}
) values (
    {% for col in adapter.get_columns_in_relation(source_relation) %}
        src.{{ col.name }}{% if not loop.last %}, {% endif %}
    {% endfor %}
)

{% endmacro %}