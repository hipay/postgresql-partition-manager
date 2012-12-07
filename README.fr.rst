Partmgr
=======
Partmgr est un ensemble de tables et de procédures stockées écrites pour
faciliter la gestion des tables partitionnées dans les bases de données.

La clé de partitionnement est une date (Année, Mois, Semaine ou Jour), 
correspondant à un attribut de la table. Il est possible de déterminer
une période de rétention.

Les tables de PartMgr sont des catalogues contenant des informations sur 
les types de partitions, les tables concernées et les triggers associées à ces 
tables.

Les procédures stockées permettent de gérer la création des triggers de 
partitionnement et des partitions, la suppression d'anciennes partitions 
et la surveillance de la présence des futures partitions.

Tables ( catalogue )
--------------------
Les tables sont dans un schéma "partition" :

  - ``partition.pattern`` : type de partitions
  - ``partition."table"`` : tables partitionnées
  - ``partition.trigger`` : triggers des tables partitionnées

La table ``pattern`` contient les différents patterns de partitionnement. Sauf lors de l'ajout 
de nouveaux patterns, il n'y a pas besoin de la modifier. Les différents pattern, et donc les 
types de partitions, sont : ``year``, ``month``, ``week``, et ``day``.

La table ``table`` contient la liste des tables que l'utilisateur souhaite partitionner. 
Elle doit renseignée par l'utilisateur.

La table ``trigger`` contient la liste des triggers associés aux tables partitionnées. Elle est
renseignée automatiquement lors de la configuration d'une nouvelle table partitionnée. 

Liste des fonctions
--------------------
  - ``fonction partition.between()`` : determine le nombre d'unités comprises 
      entre deux dates pour un type de partition donné
  - ``fonctions partition.create()`` : créer les partitions 

    -  ``partition.create()`` : fonction générique, qui crée les partitions de toutes les tables, à la date courante.
    -  ``partition.create( date)`` : crée les partitions pour toutes les tables à la date indiquée.
    -  ``partition.create( begin_date, end_date )`` : crée les partitions pour toutes les tables pour la période bornée par les dates.
    -  ``partition.create( schema, table, begin_date, end_date )`` : crée les partitions pour la table indiquée et pour la période bornée par les dates. 
    -  ``partition.create( schema, table, column, period, pattern, begin_date, end_date )`` : fonction bas niveau, appelée par les autres fonctions ``create``, pour créer les partitions selon les parametres donnés. 

  - ``function partition.create_next()`` : crée les prochaines partitions de toutes les tables. La prochaine période dépend de la date courante, à laquelle on ajoute l'intervalle ``next_part`` du pattern ( i.e. : current_date + 7jours pour un partitionnement mensuel ). Cette fonction est destinée à être appelé quotidiennement dans un planificateur tel que cron. 
  - ``function partition.drop()`` : supprimme les partitions, si la configuration le permet
  - ``function partition.check_next_part()`` : destiné à être appelée par une sonde Nagios, 
      permet de surveiller la présence des prochaines partitions utiles

  - ``function partition.grant_replace( p_acl text, p_grant text, p_ext_grant text )``
  - ``function partition.grant( acl text, tablename text )``
  - ``function partition.setgrant( p_schemaname text, p_tablename text, p_part text )``
     : utilisé par ``partition.create()`` pour appliquer les privilèges 
       lors de la création des partitions

  - ``function partition.create_part_trigger()`` : crée les fonctions triggers de partitionnement, 
      et installe le trigger sur la table mère, 
  - ``function partition.set_trigger_def()`` : Fonction trigger permettant de copier les triggers 
      de la table mère sur les partitions. Déclenché à l'insertion dans ``partition."table"``

Tutoriel
````````

Installation
::::::::::::

L'installation de partmgr se fait executant le script ``partition.sql``. 
Il crée le schéma, les tables et fonctions, et remplit les tables de références ::

  $ cd partmgr 
  $ psql -U postgres dbname < partition.sql

Configuration
:::::::::::::

Il y a 2 opérations necessaire. La première est l'insertion des tables à partitionner dans ``partition."table"`` ::

  INSERT INTO partition."table" ( schemaname, tablename, keycolumn, pattern, actif, cleanable, retention_period)
    values ('test', 'test1mois', 'ev_date', 'M', 't', 'f', null),
           ('test', 'test_mois', 'ev_date', 'M', 't', 't', '1 mon') ;

Les triggers présent sur ces tables sont enregistrés dans la table ``partition.trigger`` pour être 
automatiquement ajouté sur les partitions. À noter que ces triggers ne seront plus présent sur la table mère.

Les privilèges définis sur la table mère sont automatiquement appliqués sur les partitions.

Puis, la création et l'installation du trigger de partitionnement ::

  SELECT partition.create_part_trigger('schema_name','table_name');

Cette fonction génère la fonction trigger spécifique à la table passée en parametre. 
La fonction trigger est crée dans le schéma ``partition`` et le trigger ``_partitionne`` 
est créé sur la table. 

Création des partitions
:::::::::::::::::::::::

Ensuite, l'ensemble des partitions peuvent être crées avec les fonctions ``partition.create()`` ::
  
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


puis supprimées avec la fonction ``partition.drop()`` ::
  
  part=$ select * from partition.drop() ;
   o_tables 
  ----------
          0
  (1 row)

Seules les partitions ``cleanable`` et dont la période de rétention est passée seront supprimées. 


Planifier la création
:::::::::::::::::::::

La création des prochaines partitions, celle du mois prochain ou du jour prochain, peut être
créé simplement avec la fonction ``partition.create_next()`` . Cette fonction s'appuie sur la
colonne ``next_part`` de la table ``partition.pattern`` pour déterminer la date de la partition
a créer. 

Monitoring
::::::::::

La fonction ``partition.check_next_part()`` permet la surveillance depuis Nagios :: 
  
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

