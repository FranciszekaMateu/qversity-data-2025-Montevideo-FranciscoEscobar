-- ----------------------------------------------------------------------------------
-- Macro: clean_device_models
-- Description: Standardizes device models to 'Model <n>' format.
--              Returns 'Model Unknown' if a model cannot be inferred.
-- ----------------------------------------------------------------------------------
{% macro clean_device_models(device_model_field) %}
    {%- set digits_expr -%}
        REGEXP_REPLACE({{ device_model_field }}, '[^0-9]', '', 'g')
    {%- endset -%}

    CASE
        -- Already correct format -------------------------------------------------------
        WHEN {{ device_model_field }} ~ '^Model\s+[0-9]+$'
        THEN {{ device_model_field }}

        -- Variants like 'Model-X123' ----------------------------------------------
        WHEN {{ device_model_field }} ~ '^Model[-\s]*[0-9]+'
        THEN CONCAT('Model ', {{ digits_expr }})

        -- Starts with 'Model' but irregular format ----------------------------
        WHEN {{ device_model_field }} ~ '^Model'
        THEN CASE WHEN {{ digits_expr }} != ''
                  THEN CONCAT('Model ', {{ digits_expr }})
                  ELSE 'Model Unknown'
             END

        -- Numbers only --------------------------------------------------------------
        WHEN {{ device_model_field }} ~ '^[0-9]+$'
        THEN CONCAT('Model ', {{ device_model_field }})

        -- Null, empty or unknown markers ------------------------------------
        WHEN {{ device_model_field }} IS NULL
             OR TRIM({{ device_model_field }}) = ''
             OR LOWER(TRIM({{ device_model_field }})) IN ('unknown','n/a','null')
        THEN 'Model Unknown'

        -- Fallback ------------------------------------------------------------------
        ELSE CASE WHEN {{ digits_expr }} != ''
                   THEN CONCAT('Model ', {{ digits_expr }})
                   ELSE 'Model Unknown'
             END
    END
{% endmacro %} 