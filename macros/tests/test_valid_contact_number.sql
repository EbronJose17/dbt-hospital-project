{% test is_valid_contact_number(model, column_name) %}

select
    {{column_name}}
from 
    {{model}}
where
    len( regexp_replace( {{column_name}}, '[s-\]', '') ) <> 10

{% endtest %}

