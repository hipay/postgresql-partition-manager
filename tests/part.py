#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, psycopg2, psycopg2.extensions, time

dbhost="localhost"
dbport="5432"
dbuser="postgres"
dbname="part"

filename="../partition.sql" 
cmd = "psql -U %s -h %s -p %s -f %s %s  > /dev/null 2>&1"  % (dbuser, dbhost, dbport, filename, dbname)
os.popen(cmd)

#  > /dev/null 2>&1

filename="parttesttable.sql"
cmd = "psql -U %s -h %s -p %s -f %s %s  > /dev/null 2>&1"  % (dbuser, dbhost, dbport, filename, dbname)
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

print "Test  An  : %s " % ( avg1an )
print "Test Mois : %s " % ( avg1mois ) 
print "Test Jour : %s " % ( avg1jour )

