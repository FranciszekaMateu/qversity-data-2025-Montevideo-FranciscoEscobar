-- ----------------------------------------------------------------------------------
-- Macro: clean_country_names
-- Description: Normalizes Latin American country names. Returns 'Desconocido'
--              if the country is not recognized.
-- ----------------------------------------------------------------------------------
{% macro clean_country_names(country_column) %}
  {%- set c -%}LOWER(TRIM({{ country_column }})){%- endset -%}
  CASE
      -- Argentina ---------------------------------------------------------
      WHEN {{ c }} IN ('ar','arg','argentina','argentin','argentna') OR {{ c }} ~ '^arg.*'
          THEN 'Argentina'

      -- México ------------------------------------------------------------
      WHEN {{ c }} IN ('mx','mex','mexico','méx','mejico','mexco') OR {{ c }} ~ '^(mex|méx).*'
          THEN 'México'

      -- Colombia ----------------------------------------------------------
      WHEN {{ c }} IN ('co','col','colombia','colombi','colomia','colmbia') OR {{ c }} ~ '^col.*'
          THEN 'Colombia'

      -- Perú --------------------------------------------------------------
      WHEN {{ c }} IN ('pe','per','peru','pru','pero') OR {{ c }} ~ '^(per|pru).*'
          THEN 'Perú'

      -- Chile -------------------------------------------------------------
      WHEN {{ c }} IN ('cl','chl','chile','chle','chil','chi') OR {{ c }} ~ '^ch[il].*'
          THEN 'Chile'

      -- Fallback ----------------------------------------------------------
      ELSE 'Desconocido'
  END
{% endmacro %} 