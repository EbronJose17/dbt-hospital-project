select
    *
from 
    {{source('hospital_db', 'appointments')}}