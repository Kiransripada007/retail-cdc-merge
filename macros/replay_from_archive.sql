{% macro replay_from_archive(queue_table_name, hook_type) %}

    {% if var('is_replay', false) %}

        {% if hook_type == 'pre' %}

            {{ log("replay_from_archive [pre-hook]: detaching SLP from " ~ queue_table_name, info=True) }}
            {{ log("Replay window: " ~ var('replay_watermark') ~ " → " ~ var('replay_end'), info=True) }}
            {{ detach_slp(queue_table_name) }}

        {% elif hook_type == 'post' %}

            {{ log("replay_from_archive [post-hook]: re-attaching SLP to " ~ queue_table_name, info=True) }}
            {{ attach_slp(queue_table_name) }}

        {% else %}

            {{ exceptions.raise_compiler_error(
                "replay_from_archive: hook_type must be 'pre' or 'post', got '" ~ hook_type ~ "'"
            ) }}

        {% endif %}

    {% endif %}

{% endmacro %}