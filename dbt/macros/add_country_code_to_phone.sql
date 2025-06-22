-- ----------------------------------------------------------------------------------
-- Macro: add_country_code_to_phone
-- Description: Normalizes a phone number and adds the appropriate international
--              prefix according to the customer's country.
-- ----------------------------------------------------------------------------------
{% macro add_country_code_to_phone(phone_field, country_field) %}

    {%- set digits_expr -%}
        REGEXP_REPLACE(
            REGEXP_REPLACE({{ phone_field }}, '[^0-9]', '', 'g'),  -- digits only
            '^0+',                                                -- remove leading zeros
            ''
        )
    {%- endset -%}

    CASE
        -- Null or empty values ---------------------------------------------------
        WHEN {{ phone_field }} IS NULL
             OR TRIM({{ phone_field }}) = ''
             OR LOWER(TRIM({{ phone_field }})) IN ('unknown', 'n/a')
        THEN NULL

        -- Already includes international prefix ----------------------------------------
        WHEN {{ phone_field }} ~ '^\+[0-9]'
        THEN CONCAT('+', {{ digits_expr }})

        -- Prefix by country ---------------------------------------------------------
        WHEN {{ country_field }} = 'Argentina' THEN CONCAT('+54', {{ digits_expr }})
        WHEN {{ country_field }} = 'México'     THEN CONCAT('+52', {{ digits_expr }})
        WHEN {{ country_field }} = 'Colombia'   THEN CONCAT('+57', {{ digits_expr }})
        WHEN {{ country_field }} = 'Perú'       THEN CONCAT('+51', {{ digits_expr }})
        WHEN {{ country_field }} = 'Chile'      THEN CONCAT('+56', {{ digits_expr }})

        -- Generic fallback --------------------------------------------------------
        ELSE CONCAT('+1', {{ digits_expr }})
    END

{% endmacro %} 