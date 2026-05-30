-- =============================================================================
-- soft_delete_base
-- =============================================================================
-- Purpose:
--   Generates the base SELECT for a Silver model that must surface soft-deleted
--   records rather than physically removing them.
--
--   In the CDC pipeline, a hard delete arrives as load_operation = 'D'.
--   cdc_merge propagates that flag into the Silver table and retains the row so
--   downstream consumers can react to it. soft_delete_base provides a
--   standardised query layer that:
--
--     • Adds IS_DELETED  boolean  – true when load_operation = 'D' and the row
--                                   is NOT an archive resurrection (ARCHCA22 / U).
--     • Adds DELETED_AT  timestamp – load_timestamp_utc at deletion, else NULL.
--     • Optionally filters deleted rows out for consumers that only need live
--       records (exclude_deleted = true).
--
--   Archive-resurrected rows (data_source = 'ARCHCA22', load_operation = 'U')
--   are explicitly treated as LIVE — cdc_merge already converts them from a
--   delete+archive pair into a U, and this macro honours that intent.
--
-- Parameters:
--   model_relation    – relation to wrap; typically ref('...') or this
--   exclude_deleted   – boolean, default false
--                       true  → WHERE IS_DELETED = false (active-records view)
--                       false → all rows including soft-deleted ones
--   deleted_operation – load_operation value signalling a delete (default 'D')
--   archive_source    – data_source value for archive rows (default 'ARCHCA22')
--                       rows with this source are never flagged as deleted
--
-- Usage:
--   -- All rows (deleted rows included, flagged via IS_DELETED)
--   {{ soft_delete_base(ref('shipment_accessorial')) }}
--
--   -- Active rows only
--   {{ soft_delete_base(ref('shipment_accessorial'), exclude_deleted=true) }}
-- =============================================================================

{% macro soft_delete_base(
    model_relation,
    exclude_deleted   = false,
    deleted_operation = 'D',
    archive_source    = 'ARCHCA22'
) %}

with base as (

    select *
    from {{ model_relation }}

),

with_soft_delete_flags as (

    select
        base.*,

        -- True only for genuine deletes; archive resurrections (ARCHCA22/U)
        -- are live records and must not be flagged as deleted.
        case
            when upper(trim(load_operation)) = '{{ deleted_operation }}'
             and upper(trim(data_source))   != '{{ archive_source }}'
            then true
            else false
        end as IS_DELETED,

        -- Timestamp of deletion; NULL while the record is alive.
        case
            when upper(trim(load_operation)) = '{{ deleted_operation }}'
             and upper(trim(data_source))   != '{{ archive_source }}'
            then load_timestamp_utc
            else null
        end as DELETED_AT

    from base

)

select *
from with_soft_delete_flags
{% if exclude_deleted %}
where IS_DELETED = false
{% endif %}

{% endmacro %}
