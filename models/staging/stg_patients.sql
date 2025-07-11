select 
    *
from 
    {{source('hospital_db', 'patients')}}