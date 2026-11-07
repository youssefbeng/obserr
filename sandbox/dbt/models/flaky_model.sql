-- Échoue à la demande : SANDBOX_DBT_FAIL=1 provoque une erreur de conversion à l'exécution.
{{ config(materialized='table') }}
{% if env_var('SANDBOX_DBT_FAIL', '0') == '1' %}
select cast('not_a_number' as integer) as id
{% else %}
select 2 as id
{% endif %}
