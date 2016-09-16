################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import Connector, DEF_DBNAME, DEF_DBHOST
from ledama.leddb.Naming import *

class CreateLEDDBPartitions(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        
        # the information
        information = 'create a LEDDB'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def getPartitionDescription(self, partitionName):
        (partIndex, lowerMSId, upperMSId) = partitionName.split('_')[-3:]
        return (int(partIndex), int(lowerMSId), int(upperMSId))
    
    def createPartition(self, cursor, diagTable, partIndex, maxMSId):
        minMSId = maxMSId-lconfig.LEDDB_MS_PART
        rangeName = "_part_" + ('%03d' % partIndex) + "_" + str(minMSId) + '_' + str(maxMSId)
        partName = diagTable + rangeName
        print '    Creating ' + partName
        cursor.execute("""CREATE TABLE """ + partName + """ (
                               CHECK ( """ + MS + ID + """ >= %s AND """ + MS + ID + """ < %s )
                          ) INHERITS (""" + diagTable + """)""", [minMSId, maxMSId])
        # Add the primary keys and the foreign key to the MS table and create index to MS_ID
        cursor.execute("""ALTER TABLE """ + partName + """ ADD PRIMARY KEY (""" + diagTable + ID + """)""")
        cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_ms_id_fk FOREIGN KEY (""" + MS + ID + """) REFERENCES """ + MS + """ (""" + MS + ID + """) MATCH FULL""")
        cursor.execute("""CREATE INDEX """ + partName + """_""" + MS + ID + """ ON """ + partName + """ (""" + MS + ID + """)""")
        # Depending on the diag table type we create some constraint
        if diagTable == GAIN:
            cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_station_fk FOREIGN KEY (""" + STATION + """) REFERENCES """ + STATION + """ (""" + NAME + """) MATCH FULL""")
            cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_ms_station_dire_key UNIQUE (""" + MS + ID + """,""" + STATION + """,""" + DIRRA + """,""" + DIRDEC + """)""")
        else: #a quality table
            cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_qk_fk FOREIGN KEY (""" + QKIND + ID + """) REFERENCES """ + QKIND + """ (""" + QKIND + ID + """) MATCH FULL""")
            if diagTable == QBSTAT:
                cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_ms_qk_baseline_key UNIQUE (""" + MS + ID + """,""" + QKIND + ID + """,""" + BASELINE + ID + """)""")
            else:
                cursor.execute("""ALTER TABLE """ + partName + """ ADD CONSTRAINT """ + partName + """_ms_qk_key UNIQUE (""" + MS + ID + """,""" + QKIND + ID + """)""")
        cursor.execute("""GRANT SELECT ON """ + partName + """ TO public""")
    
        cursor.execute("""CREATE RULE """ + diagTable + """_insert""" + rangeName + """ AS
                          ON INSERT TO """ + diagTable + """ WHERE
                          ( """ + MS + ID + """ >= %s AND """ + MS + ID + """ < %s )
                          DO INSTEAD
                          INSERT INTO """ + partName + """ VALUES ( NEW.* )""", [minMSId, maxMSId])
        
    def process(self, dbname, dbuser, dbhost):
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        
        # Get the cursor
        cursor = connection.cursor()
        
        cursor.execute('select last_value from ' + MS + SEQ)
        latestMSId = cursor.fetchone()[0]
        
        for diagTable in (GAIN, QTSTAT, QBSTAT, QFSTAT):
            print diagTable
            partOk = False
            while not(partOk):
                cursor.execute('select tablename from pg_tables where tablename ~ %s order by tablename', [diagTable.lower()+"_part_*",])
                partNames = cursor.fetchall()
                numParts = len(partNames)
                # If we do not have any partition table or we detect that the latest table is already in 80% of its capacity
                if numParts == 0:
                    self.createPartition(cursor, diagTable, numParts, lconfig.LEDDB_MS_PART)
                else:
                    lastPartName = partNames[-1][0]
                    lastPartUpperMSId = self.getPartitionDescription(lastPartName)[-1]
                    if (latestMSId >= lastPartUpperMSId) or (lastPartUpperMSId - latestMSId <= (0.5 * lconfig.LEDDB_MS_PART)):
                        self.createPartition(cursor, diagTable, numParts, lastPartUpperMSId + lconfig.LEDDB_MS_PART)
                    else:
                        partOk = True
                connection.commit()
        cursor.close()
        connection.close()