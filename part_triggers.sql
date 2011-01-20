
create or replace function partition.create_part_trigger
  (
    p_schemaname text,
    p_tablename  text
  )
returns void
language plpgsql 
as $BODY$
declare
  part text ;
  v_insert text ; 
  v_column text ;
  v_pattern text ; 
begin 
 
  select t.keycolumn, p.to_char_pattern into v_column, v_pattern 
    from partition.table t join partition.pattern p on t.pattern=p.id 
    where t.schemaname = p_schemaname and t.tablename = p_tablename ;  
  if FOUND then

    execute 'create or replace function partition.partitionne_'||p_schemaname||'_'||p_tablename||'()
returns trigger
language plpgsql 
as $PART$
 declare
   part text ;
   v_column text ;
   v_pattern text ; 
 begin 
   IF TG_OP = ''INSERT''
   THEN

     SELECT to_char( NEW.'|| v_column ||', ' || quote_literal( v_pattern  ) ||  ') INTO part ;
 
     execute ''INSERT INTO '' || TG_TABLE_SCHEMA || ''.'' || TG_TABLE_NAME || ''_'' || part 
       || '' SELECT ('' || quote_literal(textin(record_out(NEW)))
       || ''::'' || TG_TABLE_SCHEMA || ''.'' || TG_TABLE_NAME || '').*;'' ;
 
     RETURN null ;
   else
     return new ; 
   END IF;
 end ; 
$PART$ ; ' ; 

  execute 'create trigger _partitionne before insert on '
      ||p_schemaname||'.'||p_tablename||' for each row '
      ||'execute procedure partition.partitionne_' 
      ||p_schemaname||'_'||p_tablename||'() ; ' ; 

  end if ; 


end ;
$BODY$
; 
