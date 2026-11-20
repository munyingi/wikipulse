{#
  Row-level security: analysts see only the languages granted to them in seeds/analyst_language_access.csv
  ('*' = all languages). Dynamic data masking: editor names are partially masked for anyone without UNMASK.
#}

{% macro drop_security_policies() %}
    {% if execute %}
    IF EXISTS (SELECT 1 FROM sys.security_policies WHERE name = 'language_policy')
        DROP SECURITY POLICY security.language_policy;
    IF OBJECT_ID('security.fn_language_access') IS NOT NULL
        DROP FUNCTION security.fn_language_access;
    {% endif %}
{% endmacro %}

{% macro create_security_policies() %}
    {% if execute and flags.WHICH in ('run', 'build') %}
    EXEC('CREATE FUNCTION security.fn_language_access(@language AS varchar(50))
        RETURNS TABLE
        WITH SCHEMABINDING
        AS RETURN
            SELECT 1 AS allowed
            FROM security.analyst_language_access AS a
            WHERE a.analyst_upn = USER_NAME() AND (a.language = @language OR a.language = ''*'')');
    CREATE SECURITY POLICY security.language_policy
        ADD FILTER PREDICATE security.fn_language_access(language) ON marts.fct_wiki_daily_traffic,
        ADD FILTER PREDICATE security.fn_language_access(language) ON marts.fct_top_articles_daily,
        ADD FILTER PREDICATE security.fn_language_access(language) ON marts.fct_editor_activity_daily
        WITH (STATE = ON);
    {% endif %}
{% endmacro %}

{% macro mask_column(column, mask) %}
    ALTER TABLE {{ this }} ALTER COLUMN {{ column }} ADD MASKED WITH (FUNCTION = '{{ mask }}')
{% endmacro %}
