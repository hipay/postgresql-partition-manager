#!/bin/bash

ok=$(psql "service=partmgr" -At -c "select date_part('day',  date_trunc('month',current_date) + interval '1 month' - next_part ) = date_part('day', current_date )  from partition.pattern where id='M';")

if [ "$ok" = "t" ] 
then
    exit 0
else 
    exit 1
fi 
