-- ----------------------------------------------------------------------------------
-- Macro: clean_device_brands
-- Description: Normalizes device brand names to standardized values.
--              Returns 'Unknown' when the brand is not recognized.
-- ----------------------------------------------------------------------------------
{% macro clean_device_brands(brand_column) %}
  {%- set b -%}LOWER(TRIM({{ brand_column }})){%- endset -%}
  CASE
      -- Samsung -----------------------------------------------------------
      WHEN {{ b }} IN ('samsung','samsun','samsg','samung') OR {{ b }} ~ '^sam.*'
          THEN 'Samsung'

      -- Apple -------------------------------------------------------------
      WHEN {{ b }} IN ('apple','aple','appl','appel') OR {{ b }} ~ '^app.*'
          THEN 'Apple'

      -- Xiaomi ------------------------------------------------------------
      WHEN {{ b }} IN ('xiaomi','xiomi','xiami','xaomi') OR {{ b }} ~ '^xia.*'
          THEN 'Xiaomi'

      -- Huawei ------------------------------------------------------------
      WHEN {{ b }} IN ('huawei','hauwei','huwai','huwei') OR {{ b }} ~ '^hua.*'
          THEN 'Huawei'

      -- Fallback ----------------------------------------------------------
      ELSE 'Unknown'
  END
{% endmacro %} 