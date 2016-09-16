#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import sys
from ledama.leddb.Connector import Connector
from ledama.leddb.Naming import *    
from ledama.leddb.modules.CreateLEDDBPartitions import CreateLEDDBPartitions

namedcursor = True
NUM_BUNCH = 50
(OLD_DB_NAME,OLD_DB_USER,OLD_DB_HOST) = ('leddb_prod', 'lofardata', 'node078')
(NEW_DB_NAME,NEW_DB_USER,NEW_DB_HOST) = ('leddb', 'lofardata', 'node001')

# After a backup and restore of LEDDB (it takes several days) we also need to update in the new leddb
# whatever changes have happened during the past days. So, we need to run this script in some moment
# we know the leddb is not being updated and it will make both leddb exactly the same

def getAuxQuery(row):
    aux = []
    for i in range(len(row)): aux.append('%s')
    return aux
        
connection_old = Connector(OLD_DB_NAME,OLD_DB_USER,OLD_DB_HOST).getConnection()
connection_new = Connector(NEW_DB_NAME,NEW_DB_USER,NEW_DB_HOST).getConnection()

cursor_old = connection_old.cursor()
cursor_new = connection_new.cursor()

for table in (PROJECT, ANTTYPE, STATION, FIELD, STORE, HOST, PARENTPATH, LDS):
    print 'Sync. ' + table + '...'
    cursor_old.execute('select * from ' + table + ' ORDER BY ' + NAME)
    cursor_new.execute('select ' + NAME + ' from ' + table + ' ORDER BY ' + NAME)
    if cursor_old.rowcount != cursor_new.rowcount:
        # There are some new rows 
        rows_table_old = cursor_old.fetchall()
        rows_table_new = cursor_new.fetchall() 
        names_new = []
        for (name, ) in rows_table_new:
            names_new.append(name)
            
        for row_table_old in rows_table_old:
            if row_table_old[0] not in names_new:
                print cursor_new.mogrify('INSERT INTO ' + table + ' VALUES (' + ','.join(getAuxQuery(row_table_old)) + ')', row_table_old)
                cursor_new.execute('INSERT INTO ' + table + ' VALUES (' + ','.join(getAuxQuery(row_table_old)) + ')', row_table_old)
                connection_new.commit()
                if table == LDS:
                    cursor_old.execute('select * from ' + LDSHASSTATION + ' WHERE ' + LDS + ' = %s', [row_table_old[0]])
                    rows_lds_has_station = cursor_old.fetchall()
                    for row_lds_has_station in rows_lds_has_station:
                        print cursor_new.mogrify('INSERT INTO ' + LDSHASSTATION + ' VALUES (' + ','.join(getAuxQuery(row_lds_has_station)) + ')', row_lds_has_station)
                        cursor_new.execute('INSERT INTO ' + LDSHASSTATION + ' VALUES (' + ','.join(getAuxQuery(row_lds_has_station)) + ')', row_lds_has_station)
                        connection_new.commit()
                        
# UDPATE LDS

tDict = {}
tDict[LDS] = [NAME,PROJECT,ANTTYPE,INITIALMJD,FINALMJD,INITIALUTC,FINALUTC]
tDict[LDSB] = [LDSB+ID,LDS,BEAMINDEX,FIELD,PHASEDIRRA,PHASEDIRDEC]
tDict[LDSBP] = [LDSBP + ID,LDSB + ID ,STORE,INTTIME,NUMCHAN,VERSION,RAW,TAR,BVF,DESCR,MAINUSER,ADDDATE,FLAGGED,AVERAGED,CALIBRATED,DIRDEPCAL,DIRINDEPCAL]                
tDict[MS] = [MS + ID,LDSBP + ID,SBINDEX,CENTFREQ ,BW]
# tDict[MSP] = [MSP + ID,MS + ID,HOST,PARENTPATH,NAME,LASTCHECK,LASTMODIFICATION,SIZE] # This will be updated from the existing files
tDict[LDSMETA] = [LDSMETA+ID,LDSNUMBEAMS,LDSDURATION,LDSNUMSTATIONS,LDSNUMSB,LDSMINCENTFREQ,LDSMAXCENTFREQ,LDSNUMMSP,LDSTOTALSIZE,LDSHASGAIN,LDSHASQUALITY,LDSHASGAINMOVIE,LDSMAINFIELD,LDSFIELDS]
tDict[LDSBMETA] = [ LDSBMETA + ID,LDSBNUMSB,LDSBNUMLDSBP,LDSBMINCENTFREQ,LDSBMAXCENTFREQ,LDSBNUMMSP,LDSBTOTALSIZE,LDSBHASGAIN,LDSBHASQUALITY,LDSBHASGAINMOVIE]
tDict[LDSBPMETA] = [LDSBPMETA + ID,LDSBPMINCENTFREQ,LDSBPMAXCENTFREQ,LDSBPNUMMSP,LDSBPTOTALSIZE,LDSBPLASTMODIFICATION,LDSBPHASGAIN,LDSBPHASQUALITY,LDSBPHASGAINMOVIE]
tDict[MSMETA] = [MSMETA + ID,MSHASMSP,MSHASGAIN,MSHASQUALITY,MSHASGAINMOVIE]

tDict[BASELINE] = [BASELINE + ID,STATION1,STATION2]
tDict[APPRUN] = [APPRUN + ID,LDSBP + ID,ORDERINDEX,APPNAME,DESCR]
tDict[APPFILE] = [APPFILE + ID,APPRUN + ID,FILEPATH]
tDict[QKIND] = [QKIND + ID,NAME,DESCR]

for table in [LDS,LDSB,LDSBP,MS,LDSMETA,LDSBMETA,LDSBPMETA,MSMETA,BASELINE,APPRUN,APPFILE,QKIND]:    
    cols = tDict[table]
    idcol = cols[0]
    cursor_old.execute('select ' + ','.join(cols) + ' from ' + table + ' ORDER BY ' + idcol)
    for row in cursor_old:
        idrow = row[0]
        urow = row[1:] + row[0:1]
        cursor_new.execute('select ' + idcol + ' from ' + table + ' WHERE ' + idcol + '=%s', (idrow,))
        rows_table_new = cursor_new.fetchall()
        if len(rows_table_new):
            print 'Updating ' + table + ' ' + str(idrow)
            cursor_new.execute('UPDATE ' + table + ' SET ' + '=%s,'.join(cols[1:]) + '=%s' + ' WHERE ' + idcol + '=%s', urow)
        else:
            print 'Inserting ' + table + ' ' + str(idrow)
            cursor_new.execute('INSERT INTO ' + table + ' (' + ','.join(cols) + ') VALUES (' + ','.join(getAuxQuery(row)) + ')', row)
        connection_new.commit()
    if table not in (LDS,LDSMETA,LDSBMETA,LDSBPMETA,MSMETA):
        # Set the sequence in the new leddb to the same valie than old leddb sequence
        cursor_old.execute('select max(' + table+ID + ') from ' + table)
        maxId_old = cursor_old.fetchall()[-1][0]
        print cursor_new.mogrify('ALTER SEQUENCE ' + table+SEQ + ' RESTART WITH %s', [maxId_old+1,])
        cursor_new.execute('ALTER SEQUENCE ' + table+SEQ + ' RESTART WITH %s', [maxId_old+1,])
        connection_new.commit()
   
CreateLEDDBPartitions().process(NEW_DB_NAME,NEW_DB_USER,NEW_DB_HOST)
   
for table in (QFSTAT, QBSTAT, QTSTAT, GAIN):
    print 'Sync. ' + table + '...'
    print
    cursor_new.execute('select tablename from pg_tables where tablename ~ %s order by tablename', [table.lower()+"_part_*",])
    partNames = cursor_new.fetchall()
    numParts = len(partNames)
    if numParts:
        tIndex = 0
        maxId_new = None
        while (maxId_new == None) and tIndex <= numParts:
            tIndex+=1
            cursor_new.execute('select max(' + table+ID + ') from ' + partNames[-tIndex][0])
            maxId_new = cursor_new.fetchall()[-1][0]
    else:
        cursor_new.execute('select max(' + table+ID + ') from ' + table)
        maxId_new = cursor_new.fetchall()[-1][0]
        
    if maxId_new != None:
        if namedcursor:
            cursor_old.close()
            cursor_old = connection_old.cursor('named')
            cursor_old.execute('select *  from ' + table + ' WHERE ' + table+ID + ' > %s', [maxId_new, ])
            goOn = True
            counter = 0
            while goOn:
                rows_old = cursor_old.fetchmany(NUM_BUNCH)
                if len(rows_old):
                    for row_old in rows_old:
                        try:
                            cursor_new.execute('INSERT INTO ' + table + ' VALUES (' + ','.join(getAuxQuery(row_old)) + ')', row_old)
                            connection_new.commit()
                        except:
                            print "DUPLICATED"
                            cursor_new.close()
                            connection_new.close()
                            connection_new = Connector(NEW_DB_NAME,NEW_DB_USER,NEW_DB_HOST).getConnection()
                            cursor_new = connection_new.cursor()
                        counter += 1
                        print counter
                else:
                    goOn = False
            cursor_old.close()
            cursor_old = connection_old.cursor()
        else:
            cursor_old.execute('select '+ table+ID +'  from ' + table + ' WHERE ' + table+ID + ' > %s', [maxId_new, ])
            tids = cursor_old.fetchall()
            numtids = len(tids)
            for i in range(numtids):
                cursor_old.execute('select * from ' + table + ' WHERE ' + table+ID + ' = %s', [tids[i], ])
                row_old  = cursor_old.fetchone()
                try:
                    cursor_new.execute('INSERT INTO ' + table + ' VALUES (' + ','.join(getAuxQuery(row_old)) + ')', row_old)
                    connection_new.commit()
                except:
                    print "DUPLICATED"
                    cursor_new.close()
                    connection_new.close()
                    connection_new = Connector(NEW_DB_NAME,NEW_DB_USER,NEW_DB_HOST).getConnection()
                    cursor_new = connection_new.cursor()
                sys.stdout.write("\r %3.1f%% " % (float(i*100.) / float(numtids)))


        # Set the sequence in the new leddb to the same valie than old leddb sequence

        cursor_old.execute('select max(' + table+ID + ') from ' + table)
        maxId_old = cursor_old.fetchall()[-1][0]
        print cursor_new.mogrify('ALTER SEQUENCE ' + table+SEQ + ' RESTART WITH %s', [maxId_old+1,])
        cursor_new.execute('ALTER SEQUENCE ' + table+SEQ + ' RESTART WITH %s', [maxId_old+1,])
        connection_new.commit()
