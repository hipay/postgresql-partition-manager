begin ;

insert into partition.pattern values 
    ('W','week','YYYYIW', '3 day')
;


create or replace function partition.between(
  i_period text, 
  begin_date date, 
  end_date date
) 
returns integer
language plpgsql 
as $$
declare 
  v_ret integer ;  
  v_age interval ;
begin
  if i_period = 'year'::text then
    select extract(year from age($3,$2))::integer +1  into v_ret ; 
  elsif i_period = 'month'::text then
    select age($3,$2) into v_age ; 
    select 12 * extract(year from v_age)::integer + extract(month from v_age)::integer into v_ret ; 
  elsif i_period = 'day'::text then
    select end_date::date - begin_date::date into v_ret ; 
  elsif i_period = 'week'::text then
    select (end_date::date - begin_date::date)::integer / 7 into v_ret ; 
  else
    raise exception 'period % not yet implemented', i_period ; 
  end if ; 
  return v_ret ;
end ;
$$ ;

commit ; 
