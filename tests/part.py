#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, psycopg2, psycopg2.extensions, time

dbhost="localhost"
dbport="5432"
dbuser="postgres"
dbname="part"

filename="partition.sql" 
cmd = "psql -U %s -h %s -p %s -f %s %s"  % (dbuser, dbhost, dbport, filename, dbname)
os.popen(cmd)

#  > /dev/null 2>&1

filename="parttesttable.sql"
cmd = "psql -U %s -h %s -p %s -f %s %s"  % (dbuser, dbhost, dbport, filename, dbname)
os.popen(cmd)

filename="partcreate.sql"
cmd = "psql -U %s -h %s -p %s -f %s %s"  % (dbuser, dbhost, dbport, filename, dbname)
os.popen(cmd)


trconn = psycopg2.connect("host=%s port=%s dbname=%s user=%s" % (dbhost, dbport, dbname, dbuser ) ) 
trconn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
trcurs = trconn.cursor() 

def average(values):
    return sum(values, 0.0) / len(values)

def execInsertPart( table, tmstps ):
    trcurs.execute("truncate table %s" % ( table )  )
    trcurs.execute(" prepare ins_date( timestamptz ) as insert into %s ( ev_date ) values ( $1::timestamptz )" % ( table ) )
    ins_time = [] 
    for tm in tmstps:
        begin = time.time()
        trcurs.execute("execute ins_date( '%s') ;" % ( tm ) ) 
        end = time.time()
        ins_time.append( end - begin )
        # if len( ins_time ) % 10000 == 0:
            # print "AvgCpcTime;%s;%s;%s;%s;%s" % ( table, str(average( ins_time )), str(min(ins_time)),str(max(ins_time)), len(ins_time) )
    trcurs.execute("deallocate ins_date ;") 

    # print "AvgCpcTimeEnd;%s;%s;%s;%s;%s" % ( table ,str(average(ins_time )), str(min(ins_time)),str(max(ins_time)), len(ins_time) )
    return average(ins_time)

tmstps = []

trcurs.execute('''select now() + ((random() - 0.5) * 20000000 )::text::interval as ev_date from generate_series(1, 200000 )  ''')
record = trcurs.fetchall() 
for tm in record:
    tmstps.append( tm[0] )

#insert into test1an ( ev_date ) values ( now() ) ;
avg1an = execInsertPart('test.test1an' , tmstps )
#insert into test1mois ( ev_date ) values ( now() ) ;
avg1mois = execInsertPart('test.test1mois' , tmstps )
#insert into test1jour ( ev_date ) values ( now() ) ;
avg1jour = execInsertPart('test.test1jour' , tmstps )

#insert into test2an ( ev_date ) values ( now() ) ;
avg2an = execInsertPart('test.test2an' , tmstps )
#insert into test2mois ( ev_date ) values ( now() ) ;
avg2mois = execInsertPart('test.test2mois' , tmstps )
#insert into test2jour ( ev_date ) values ( now() ) ;
avg2jour = execInsertPart('test.test2jour' , tmstps )

#insert into test3an ( ev_date ) values ( now() ) ;
avg3an = execInsertPart('test.test3an' , tmstps )
#insert into test3mois ( ev_date ) values ( now() ) ;
avg3mois = execInsertPart('test.test3mois' , tmstps )
#insert into test3jour ( ev_date ) values ( now() ) ;
avg3jour = execInsertPart('test.test3jour' , tmstps )

#insert into test4an ( ev_date ) values ( now() ) ;
avg4an = execInsertPart('test.test4an' , tmstps )
#insert into test4mois ( ev_date ) values ( now() ) ;
avg4mois = execInsertPart('test.test4mois' , tmstps )
#insert into test4jour ( ev_date ) values ( now() ) ;
avg4jour = execInsertPart('test.test4jour' , tmstps )

print "Test An :1[%s] > (%s,%s) >  2[%s]  > (%s,%s) > 3[%s]  > (%s,%s) > 4[%s]" % ( avg1an, avg1an - avg2an, (avg1an-avg2an)/avg2an*100, 
                                                                                    avg2an, avg2an - avg3an, (avg2an-avg3an)/avg3an*100, 
                                                                                    avg3an, avg3an - avg4an, (avg3an-avg4an)/avg4an*100, avg4an )
print "TestMois:1[%s] > (%s,%s) >  2[%s]  > (%s,%s) > 3[%s]  > (%s,%s) > 4[%s]" %  ( avg1mois, avg1mois - avg2mois, (avg1mois-avg2mois)/avg2mois*100, 
                                                                                    avg2mois, avg2mois - avg3mois, (avg2mois-avg3mois)/avg3mois*100, 
                                                                                    avg3mois, avg3mois - avg4mois, (avg3mois-avg4mois)/avg4mois*100, avg4mois ) 
print "TestJour:1[%s] > (%s,%s) >  2[%s]  > (%s,%s) > 3[%s]  > (%s,%s) > 4[%s]" %  ( avg1jour, avg1jour - avg2jour, (avg1jour-avg2jour)/avg2jour*100, 
                                                                                    avg2jour, avg2jour - avg3jour, (avg2jour-avg3jour)/avg3jour*100, 
                                                                                    avg3jour, avg3jour - avg4jour, (avg3jour-avg4jour)/avg4jour*100, avg4jour )



# Test An :1[0.000700465756655] > (8.89248919487e-05,14.5411201574) >  2[0.000611540864706]  > (0.000197200980186,47.5940134064) > 3[0.00041433988452]  > (0.000244231425524,143.573945097) > 4[0.000170108458996]
# TestMois:1[0.000707037975788] > (9.42051029205e-05,15.3720707702) >  2[0.000612832872868]  > (0.000194285348654,46.4189458578) > 3[0.000418547524214]  > (0.000249758322239,147.970556953) > 4[0.000168789201975]
# TestJour:1[0.000722788352966] > (9.45101559162e-05,15.0427241244) >  2[0.00062827819705]  > (0.000207206104994,49.2091755552) > 3[0.000421072092056]  > (0.000264163653851,168.355288518) > 4[0.000156908438206]
