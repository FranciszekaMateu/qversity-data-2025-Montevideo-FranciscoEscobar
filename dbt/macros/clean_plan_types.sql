-- ----------------------------------------------------------------------------------
-- Macro: clean_plan_types
-- Description: Normalizes phone plan types. Returns 'UNKNOWN' for
--              unrecognized values.
-- ----------------------------------------------------------------------------------
{% macro clean_plan_types(plan_type_column) %}
  {%- set p -%}LOWER(TRIM({{ plan_type_column }})){%- endset -%}
  CASE
      -- PREPAGO ------------------------------------------------------------
      WHEN {{ p }} IN ('pre','pre-pago','pre_pago','prepago') OR {{ p }} ~ '^pre.*'
          THEN 'PREPAGO'

      -- POSTPAGO -----------------------------------------------------------
      WHEN {{ p }} IN ('post-pago','post_pago','pospago','pos') OR {{ p }} ~ '^(post|pos).*'
          THEN 'POSTPAGO'

      -- CONTROL ------------------------------------------------------------
      WHEN {{ p }} IN ('ctrl','control','contrrol') OR {{ p }} ~ '^(ctrl|control|contrrol).*'
          THEN 'CONTROL'

      -- Fallback -----------------------------------------------------------
      ELSE 'UNKNOWN'
  END
{% endmacro %} 