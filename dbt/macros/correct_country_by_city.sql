-- ----------------------------------------------------------------------------------
-- Macro: correct_country_by_city
-- Description: Only corrects the country when there's inconsistency between city and country.
--              If there's no problem, maintains the original normalized country.
-- ----------------------------------------------------------------------------------
{% macro correct_country_by_city(country_normalized, city_normalized) %}
  {%- set city_clean -%}LOWER(TRANSLATE(TRIM({{ city_normalized }}), 'áéíóúüñÁÉÍÓÚÜÑ', 'aeiouunAEIOUUN')){%- endset %}
  
  CASE
    -- ------------------------------------------------------------------
    -- Only correct if inconsistent: Colombian city with non-Colombian country
    -- ------------------------------------------------------------------
    WHEN {{ city_clean }} IN ('bogota', 'bogta', 'medellin', 'medelin', 'cali', 'cal', 'barranquilla', 'barranquila')
         OR {{ city_clean }} ~ '^(bog|medel|cal$|barran).*'
    THEN 
      CASE 
        WHEN {{ country_normalized }} != 'Colombia' THEN 'Colombia'  -- Fix inconsistency
        ELSE {{ country_normalized }}  -- Keep original country if already correct
      END

    -- ------------------------------------------------------------------
    -- Only correct if inconsistent: Argentine city with non-Argentine country
    -- ------------------------------------------------------------------
    WHEN {{ city_clean }} IN ('buenos aires', 'buenos_aires', 'buenosaires', 'cordoba', 'coroba', 'rosario', 'rosrio')
         OR {{ city_clean }} ~ '^(buenos|cor.*ba$|rosar).*'
    THEN 
      CASE 
        WHEN {{ country_normalized }} != 'Argentina' THEN 'Argentina'  -- Fix inconsistency
        ELSE {{ country_normalized }}  -- Keep original country if already correct
      END

    -- ------------------------------------------------------------------
    -- Only correct if inconsistent: Mexican city with non-Mexican country
    -- ------------------------------------------------------------------
    WHEN {{ city_clean }} IN ('ciudad de mexico', 'cdmx', 'mexico city', 'guadalajara', 'guadaljara', 'gdl', 'monterrey', 'monterey', 'mty')
         OR {{ city_clean }} ~ '^(ciudad.*mex|guadal|monter).*'
    THEN 
      CASE 
        WHEN {{ country_normalized }} != 'México' THEN 'México'  -- Fix inconsistency
        ELSE {{ country_normalized }}  -- Keep original country if already correct
      END

    -- ------------------------------------------------------------------
    -- Only correct if inconsistent: Peruvian city with non-Peruvian country
    -- ------------------------------------------------------------------
    WHEN {{ city_clean }} IN ('lima', 'lma', 'arequipa', 'areqipa', 'arequpa', 'trujillo', 'trujilo', 'trjllo')
         OR {{ city_clean }} ~ '^(lim|areq|truj).*'
    THEN 
      CASE 
        WHEN {{ country_normalized }} != 'Perú' THEN 'Perú'  -- Fix inconsistency
        ELSE {{ country_normalized }}  -- Keep original country if already correct
      END

    -- ------------------------------------------------------------------
    -- Only correct if inconsistent: Chilean city with non-Chilean country
    -- ------------------------------------------------------------------
    WHEN {{ city_clean }} IN ('santiago', 'santigo', 'stgo', 'valparaiso', 'valpo', 'concepcion', 'conce')
         OR {{ city_clean }} ~ '^(sant.*go$|valpar|concep).*'
    THEN 
      CASE 
        WHEN {{ country_normalized }} != 'Chile' THEN 'Chile'  -- Fix inconsistency
        ELSE {{ country_normalized }}  -- Keep original country if already correct
      END

    -- ------------------------------------------------------------------
    -- If no recognized city, keep original normalized country
    -- ------------------------------------------------------------------
    ELSE {{ country_normalized }}
  END
{% endmacro %} 