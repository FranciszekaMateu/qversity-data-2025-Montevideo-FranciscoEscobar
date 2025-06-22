-- ----------------------------------------------------------------------------------
-- Macro: clean_cities
-- Description: Normalizes and standardizes city names 
-- ----------------------------------------------------------------------------------
{% macro clean_cities(city_column) %}
  {%- set c -%}LOWER(TRANSLATE(TRIM({{ city_column }}), 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN')){%- endset %}
  CASE
    -- ------------------------------------------------------------------
    -- Colombia 
    -- ------------------------------------------------------------------
    WHEN {{ c }} IN ('bogota', 'bogta') OR {{ c }} ~ '^bog.*'          THEN 'Bogota'
    WHEN {{ c }} IN ('medellin', 'medelin')           OR {{ c }} ~ '^medel.*'   THEN 'Medellin'
    WHEN {{ c }} IN ('cali', 'cal')                   OR {{ c }} ~ '^cal$'      THEN 'Cali'
    WHEN {{ c }} IN ('barranquilla', 'barranquila')   OR {{ c }} ~ '^barran.*'  THEN 'Barranquilla'

    -- ------------------------------------------------------------------
    -- Argentina
    -- ------------------------------------------------------------------
    WHEN {{ c }} IN ('buenos aires', 'buenos_aires', 'buenosaires') OR {{ c }} ~ '^buenos.*' THEN 'Buenos Aires'
    WHEN {{ c }} IN ('cordoba', 'coroba')                         OR {{ c }} ~ '^cor.*ba$'  THEN 'Cordoba'
    WHEN {{ c }} IN ('rosario', 'rosrio')                         OR {{ c }} ~ '^rosar.*'   THEN 'Rosario'

    -- ------------------------------------------------------------------
    -- México 
    -- ------------------------------------------------------------------
    WHEN {{ c }} IN ('ciudad de mexico', 'cdmx', 'mexico city') OR {{ c }} ~ '^ciudad.*mex.*' THEN 'Ciudad de Mexico'
    WHEN {{ c }} IN ('guadalajara', 'guadaljara', 'gdl')                           OR {{ c }} ~ '^guadal.*'      THEN 'Guadalajara'
    WHEN {{ c }} IN ('monterrey', 'monterey', 'mty')                               OR {{ c }} ~ '^monter.*'      THEN 'Monterrey'

    -- ------------------------------------------------------------------
    -- Perú (sin tildes)
    -- ------------------------------------------------------------------
    WHEN {{ c }} IN ('lima', 'lma')                    OR {{ c }} ~ '^lim.*'     THEN 'Lima'
    WHEN {{ c }} IN ('arequipa', 'areqipa', 'arequpa') OR {{ c }} ~ '^areq.*'    THEN 'Arequipa'
    WHEN {{ c }} IN ('trujillo', 'trujilo', 'trjllo')  OR {{ c }} ~ '^truj.*'    THEN 'Trujillo'

    -- ------------------------------------------------------------------
    -- Chile (sin tildes)
    -- ------------------------------------------------------------------
    WHEN {{ c }} IN ('santiago', 'santigo', 'stgo')    OR {{ c }} ~ '^sant.*go$' THEN 'Santiago'
    WHEN {{ c }} IN ('valparaiso', 'valpo')            OR {{ c }} ~ '^valpar.*'  THEN 'Valparaiso'
    WHEN {{ c }} IN ('concepcion', 'conce')            OR {{ c }} ~ '^concep.*'  THEN 'Concepcion'

    -- ------------------------------------------------------------------
    -- Fallback (sin tildes)
    -- ------------------------------------------------------------------
    ELSE INITCAP(TRANSLATE(TRIM({{ city_column }}), 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN'))
  END
{% endmacro %} 