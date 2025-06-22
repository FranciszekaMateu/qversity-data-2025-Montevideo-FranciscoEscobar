-- ----------------------------------------------------------------------------------
-- Macro: clean_operators
-- Description: Normalizes mobile operator names. Returns 'UNKNOWN' when
--              the operator is not recognized.
-- ----------------------------------------------------------------------------------
{% macro clean_operators(operator_column) %}
  {%- set op -%}LOWER(TRIM({{ operator_column }})){%- endset -%}
  CASE
      -- MOVISTAR -----------------------------------------------------------
      WHEN {{ op }} IN ('movistar','movistr','movstr','mov','movi') OR {{ op }} ~ '^mov.*'
          THEN 'MOVISTAR'

      -- TIGO ---------------------------------------------------------------
      WHEN {{ op }} IN ('tigo','tgo','tig','tygo') OR {{ op }} ~ '^(tig|tgo).*'
          THEN 'TIGO'

      -- CLARO --------------------------------------------------------------
      WHEN {{ op }} IN ('claro','cla','clar','clr') OR {{ op }} ~ '^cla.*'
          THEN 'CLARO'

      -- WOM ----------------------------------------------------------------
      WHEN {{ op }} IN ('wom','w0m','vom','wm','won') OR {{ op }} ~ '^(wom|w0m|won).*'
          THEN 'WOM'

      -- Fallback -----------------------------------------------------------
      ELSE 'UNKNOWN'
  END
{% endmacro %} 