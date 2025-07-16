{% test date_order(model, column_name, column_start, column_end) %}

select 
    *
from 
    {{model}}
where   
    {{column_start}} > {{column_end}}

{% endtest %}