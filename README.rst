Partmgr
=======
Partmgr is a set of tables and functions wrote to help the management 
of partitionning tables in PostgreSQL databases.

The partition key is a date, matching a table's attribute. It is possible
to set up a retention period 

Partmgr's tables are catalog which contains informations about partition type, tables and triggers

Functions can manage trigger creation, partition, drop, and monitoring. 

Tables ( catalog )
--------------------
Tables are in "partition" schema :

  - ``partition.pattern`` : partition's type
  - ``partition."table"`` : partitionning tables
  - ``partition.trigger`` : trigger's partitionning tables

Table ``pattern``  contiens some partitioning patterns. Until there no new pattern, 
there is no need to modify it. Different patterns, and therefore
partition types are: `` year ``, `` month ``, `` `` week, and `` day ``.

Table ``table`` contains user's tables, and must be set by the user.

La table ``trigger`` contains partition table's triggers. Filling this table is automatically 
done when the user set up a new partition table. 

Liste des fonctions
--------------------
  - ``fonction partition.between()`` : compute unit number betwenn two date fon a given pattern. 
  - ``fonctions partition.create()`` : create the partitions

    -  ``partition.create()`` : generic function, which create partitions for all tables, at the current date.
    -  ``partition.create( date)`` : create partitions for all tables, at the given date.
    -  ``partition.create( begin_date, end_date )`` : create partitions for all tables, at the given period.
    -  ``partition.create( schema, table, begin_date, end_date )`` : create partitions for the given table, at the given period. 
    -  ``partition.create( schema, table, column, period, pattern, begin_date, end_date )`` : low-level function fonction, 
called by all ``create`` functions. 

  - ``function partition.create_next()`` : create next functions of all tables. The next period depends of current date, 
plus ``next_part`` interval from pattern. This fonction could called by a scheduler like cron.
  - ``function partition.drop()`` : drop partition, if permit by setup.
  - ``function partition.check_next_part()`` : had to be called by Nagios plugin. Can monitoring if next partition exists.

  - ``function partition.grant_replace( p_acl text, p_grant text, p_ext_grant text )`` : 
  - ``function partition.grant( acl text, tablename text )`` : 
  - ``function partition.setgrant( p_schemaname text, p_tablename text, p_part text )`` : used by ``partition.create()`` to apply grants on partitions. 

  - ``function partition.create_part_trigger()`` : create partitioning triggers, and create the trigger on the partition.
  - ``function partition.set_trigger_def()`` : Trigger function which copy trigger definition from mother table to catalog. Triggered on ``partition."table"``

Tutorial
````````

Installation
::::::::::::

To install PartMgr, run the script ``partition.sql``. It make the schema, tables and functions, and fill the table ::

  $ cd partmgr 
  $ psql -U postgres dbname < partition.sql

Setup
:::::

There is two opérations needed to setup up partitionning table. One is insertion into ``partition."table"`` ::

  INSERT INTO partition."table" ( schemaname, tablename, keycolumn, pattern, actif, cleanable, retention_period)
    values ('test', 'test1mois', 'ev_date', 'M', 't', 'f', null),
           ('test', 'test_mois', 'ev_date', 'M', 't', 't', '1 mon') ;

Triggers on this table are inserted into ``partition.trigger`` to be auto-added on partition. 
These triggers won't be present on the mother table.

Privileges setted up on the mother table are automatically applied on partitions.

The second step is creation and setup of partitionning trigger ::

  SELECT partition.create_part_trigger('schema_name','table_name');

This function make the specific function trigger for the table given. The new trigger function is
created in the  ``partition`` and the trigger ``_partitionne`` is created on the table. 

Partition Creation
::::::::::::::::::

Then, the set of partition should be created with ``partition.create()`` functions ::
  
  part=$ select * from partition.create('2012-09-01','2012-11-01') ;
   o_tables | o_indexes | o_triggers | o_grants 
  ----------+-----------+------------+----------
         74 |        74 |         65 |      126
  (1 row)

  part=$ select * from partition.create('test','test_mois','2012-11-01','2013-03-01') ;
   o_tables | o_indexes | o_triggers | o_grants 
  ----------+-----------+------------+----------
          4 |         4 |          0 |        4
  (1 row)


then dropped by ``partition.drop()`` function ::
  
  part=$ select * from partition.drop() ;
   o_tables 
  ----------
          0
  (1 row)

Only partitions ``cleanable``  and whose retention period has passed will be deleted.

Schedule Creation
:::::::::::::::::

The creation of the next partitions, the next month or the next day, can be
created simply with the ``partition.create_next ()``. This function is based on the
``next_part`` column of the table ``partition.pattern`` to determine the date of the partition
to create.

Monitoring
::::::::::

``partition.check_next_part()`` function allows monitoring from Nagios ::
  
  part=$ select * from partition.check_next_part() ;
   nagios_return_code |              message              
  --------------------+-----------------------------------
                    2 | Missing : test.test1jour_20120628
  (1 row)
  part=$ select * from partition.create('test','test1jour','2012-06-28','2012-06-29') ;
   o_tables | o_indexes | o_triggers | o_grants 
  ----------+-----------+------------+----------
          2 |         2 |          2 |        4
  (1 row)
  part=$ select * from partition.check_next_part() ;
   nagios_return_code | message 
  --------------------+---------
                    0 | 
  (1 row)

