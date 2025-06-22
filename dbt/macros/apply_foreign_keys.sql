-- ----------------------------------------------------------------------------------
-- Macro: apply_foreign_keys
-- Description: Creates idempotent foreign keys between
--              `stg_payment_history` and `stg_mobile_customers_cleaned`.
-- ----------------------------------------------------------------------------------
{% macro apply_foreign_keys() %}
  {% set sql %}
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM   pg_constraint
        WHERE  conname = 'fk_payment_customer'
      ) THEN
        ALTER TABLE {{ ref('stg_payment_history') }}
        ADD CONSTRAINT fk_payment_customer
            FOREIGN KEY (customer_id)
            REFERENCES {{ ref('stg_mobile_customers_cleaned') }} (customer_id)
            ON DELETE CASCADE;
      END IF;
    END $$;
  {% endset %}

  {% do run_query(sql) %}
{% endmacro %} 