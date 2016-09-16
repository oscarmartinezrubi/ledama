################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import time
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps

class UpdateLEDDBMeta(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('nottime','n','Do not show spent time',default=False)
        options.add('complete','c','Complete',default=False, helpmessage=' update, it will also regenerate all the MS Meta-Data')
        options.add('withmsp','m','Include MSP checking',default=False)
        options.add('drop','d','Drop Tables',default=False, helpmessage=' and recreate them instead of removing all rows in the meta and joined tables')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        # the information
        information = 'update LEDDB metadata'
        # Initialize the parent class
        LModule.__init__(self, options, information, False)   
    
    def process(self, nottime, complete, withmsp, drop, dbname, dbuser, dbhost):
        
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        tstart = time.time()
        # Get the cursor
        cursor = connection.cursor()
        
        cleanTables = [LDSBPMETA, LDSBMETA, LDSMETA]
        if complete:
            cleanTables.append(MSMETA)
        
        for table in cleanTables:
            try:
                if drop:
                    cursor.execute("""DROP TABLE """ + table)
                else:
                    cursor.execute("""DELETE FROM """ + table)
                connection.commit()
            except:
                connection = Connector(dbname, dbuser, dbhost).getConnection()
                cursor = connection.cursor()

        if complete:
            if drop:
                cursor.execute("""CREATE TABLE """ + MSMETA + """ AS SELECT """ + 
                    MS+ID + """ AS """ + MSMETA+ID + """,""" +
                    MSHASMSP + """(""" + MS+ID + """),""" +
                    MSHASGAIN + """(""" + MS+ID + """),""" +
                    MSHASQUALITY  + """(""" + MS+ID + """),""" +
                    MSHASGAINMOVIE  + """(""" + MS+ID + """) from """ + MS + """;
                """)
                cursor.execute("ALTER TABLE " + MSMETA + " ADD PRIMARY KEY (" + MSMETA+ID +")")
                cursor.execute("GRANT SELECT ON " + MSMETA + " TO public")
            else:
                cursor.execute("""INSERT INTO """ + MSMETA + """ (""" + 
            MSMETA+ID + """, """ + MSHASMSP + """, """ + MSHASGAIN + """, """ + 
            MSHASQUALITY + """, """ + MSHASGAINMOVIE + """) SELECT """ + 
                    MS+ID + """,""" +
                    MSHASMSP + """(""" + MS+ID + """),""" +
                    MSHASGAIN + """(""" + MS+ID + """),""" +
                    MSHASQUALITY  + """(""" + MS+ID + """),""" +
                    MSHASGAINMOVIE  + """(""" + MS+ID + """) from """ + MS + """;
                """)
        elif withmsp:
            cursor.execute('UPDATE ' + MSMETA + ' SET ' + MSHASMSP + ' = ' + MSHASMSP + '(' + MSMETA+ID + ')')    
        connection.commit()
        
        if drop:
            cursor.execute("""CREATE TABLE """ + LDSBPMETA + """ AS SELECT """ + 
                LDSBP+ID + """ AS """ + LDSBPMETA+ID + """,""" +
                LDSBPMINCENTFREQ + """(""" + LDSBP+ID + """),""" +
                LDSBPMAXCENTFREQ + """(""" + LDSBP+ID + """),""" +
                LDSBPNUMMSP + """(""" + LDSBP+ID + """),""" +
                LDSBPTOTALSIZE + """(""" + LDSBP+ID + """),""" +
                LDSBPLASTMODIFICATION + """(""" + LDSBP+ID + """),""" +
                LDSBPHASGAIN + """(""" + LDSBP+ID + """),""" +
                LDSBPHASQUALITY + """(""" + LDSBP+ID + """),""" +
                LDSBPHASGAINMOVIE + """(""" + LDSBP+ID + """) from """ + LDSBP + """;
            """)
            cursor.execute("ALTER TABLE " + LDSBPMETA + " ADD PRIMARY KEY (" + LDSBPMETA+ID +")")
            cursor.execute("GRANT SELECT ON " + LDSBPMETA + " TO public")
        else:
            cursor.execute("""INSERT INTO """ + LDSBPMETA + """ (""" + 
            LDSBPMETA+ID + """, """ + LDSBPMINCENTFREQ + """, """ + LDSBPMAXCENTFREQ + """, """ + 
            LDSBPNUMMSP + """, """ + LDSBPTOTALSIZE + """, """ + LDSBPLASTMODIFICATION + """, """ + 
            LDSBPHASGAIN + """, """ + LDSBPHASQUALITY + """, """ + LDSBPHASGAINMOVIE + """) SELECT """ + 
                LDSBP+ID + """,""" +
                LDSBPMINCENTFREQ + """(""" + LDSBP+ID + """),""" +
                LDSBPMAXCENTFREQ + """(""" + LDSBP+ID + """),""" +
                LDSBPNUMMSP + """(""" + LDSBP+ID + """),""" +
                LDSBPTOTALSIZE + """(""" + LDSBP+ID + """),""" +
                LDSBPLASTMODIFICATION + """(""" + LDSBP+ID + """),""" +
                LDSBPHASGAIN + """(""" + LDSBP+ID + """),""" +
                LDSBPHASQUALITY + """(""" + LDSBP+ID + """),""" +
                LDSBPHASGAINMOVIE + """(""" + LDSBP+ID + """) from """ + LDSBP + """;
            """)
        connection.commit()
        
        if drop:
            cursor.execute("""CREATE TABLE """ + LDSBMETA + """ AS SELECT """ + 
                LDSB+ID + """ AS """ + LDSBMETA+ID + """,""" +
                LDSBNUMSB + """(""" + LDSB+ID + """),""" +
                LDSBNUMLDSBP + """(""" + LDSB+ID + """),""" +
                LDSBMINCENTFREQ + """(""" + LDSB+ID + """),""" +
                LDSBMAXCENTFREQ + """(""" + LDSB+ID + """),""" +
                LDSBNUMMSP + """(""" + LDSB+ID + """),""" +
                LDSBTOTALSIZE + """(""" + LDSB+ID + """),""" +
                LDSBHASGAIN + """(""" + LDSB+ID + """),""" +
                LDSBHASQUALITY  + """(""" + LDSB+ID + """),""" +
                LDSBHASGAINMOVIE  + """(""" + LDSB+ID + """) from """ + LDSB + """;
            """)
            cursor.execute("ALTER TABLE " + LDSBMETA + " ADD PRIMARY KEY (" + LDSBMETA+ID +")")
            cursor.execute("GRANT SELECT ON " + LDSBMETA + " TO public")
        else:
            cursor.execute("""INSERT INTO """ + LDSBMETA + """ (""" + 
            LDSBMETA+ID + """, """ + LDSBNUMSB + """, """ + LDSBNUMLDSBP + """, """ + 
            LDSBMINCENTFREQ + """, """ + LDSBMAXCENTFREQ + """, """ + LDSBNUMMSP + """, """ + 
            LDSBTOTALSIZE + """, """ + LDSBHASGAIN + """, """ + LDSBHASQUALITY + """, """ + 
            LDSBHASGAINMOVIE + """) SELECT """ + 
                LDSB+ID + """,""" +
                LDSBNUMSB + """(""" + LDSB+ID + """),""" +
                LDSBNUMLDSBP + """(""" + LDSB+ID + """),""" +
                LDSBMINCENTFREQ + """(""" + LDSB+ID + """),""" +
                LDSBMAXCENTFREQ + """(""" + LDSB+ID + """),""" +
                LDSBNUMMSP + """(""" + LDSB+ID + """),""" +
                LDSBTOTALSIZE + """(""" + LDSB+ID + """),""" +
                LDSBHASGAIN + """(""" + LDSB+ID + """),""" +
                LDSBHASQUALITY  + """(""" + LDSB+ID + """),""" +
                LDSBHASGAINMOVIE  + """(""" + LDSB+ID + """) from """ + LDSB + """;
            """)
        connection.commit()
        
        if drop:
            cursor.execute("""CREATE TABLE """ + LDSMETA + """ AS SELECT """ + 
                NAME + """ AS """ + LDSMETA+ID + """,""" +
                LDSNUMBEAMS + """(""" + NAME + """),""" +
                LDSDURATION + """(""" + NAME + """),""" +
                LDSNUMSTATIONS + """(""" + NAME + """),""" +
                LDSNUMSB + """(""" + NAME + """),""" +
                LDSMINCENTFREQ + """(""" + NAME + """),""" +
                LDSMAXCENTFREQ + """(""" + NAME + """),""" +
                LDSNUMMSP + """(""" + NAME + """),""" +
                LDSTOTALSIZE + """(""" + NAME + """),""" +
                LDSHASGAIN + """(""" + NAME + """),""" +
                LDSHASQUALITY + """(""" + NAME + """),""" +
                LDSHASGAINMOVIE + """(""" + NAME + """),""" +
                LDSMAINFIELD  + """(""" + NAME + """) from """ + LDS + """;
            """)
            cursor.execute("ALTER TABLE " + LDSMETA + " ADD PRIMARY KEY (" + LDSMETA+ID +")")
            cursor.execute("ALTER TABLE " + LDSMETA + " ADD COLUMN " + LDSFIELDS +" text")
            cursor.execute("GRANT SELECT ON " + LDSMETA + " TO public")
        else:
            cursor.execute("""INSERT INTO """ + LDSMETA + """ (""" + 
            LDSMETA+ID + """, """ + LDSNUMBEAMS + """, """ + LDSDURATION + """, """ + 
            LDSNUMSTATIONS + """, """ + LDSNUMSB + """, """ + LDSMINCENTFREQ + """, """ + 
            LDSMAXCENTFREQ + """, """ + LDSNUMMSP + """, """ + LDSTOTALSIZE + """, """ + 
            LDSHASGAIN + """, """ + LDSHASQUALITY + """, """ + LDSHASGAINMOVIE + """, """ + LDSMAINFIELD + """) SELECT """ + 
                NAME + """,""" +
                LDSNUMBEAMS + """(""" + NAME + """),""" +
                LDSDURATION + """(""" + NAME + """),""" +
                LDSNUMSTATIONS + """(""" + NAME + """),""" +
                LDSNUMSB + """(""" + NAME + """),""" +
                LDSMINCENTFREQ + """(""" + NAME + """),""" +
                LDSMAXCENTFREQ + """(""" + NAME + """),""" +
                LDSNUMMSP + """(""" + NAME + """),""" +
                LDSTOTALSIZE + """(""" + NAME + """),""" +
                LDSHASGAIN + """(""" + NAME + """),""" +
                LDSHASQUALITY + """(""" + NAME + """),""" +
                LDSHASGAINMOVIE + """(""" + NAME + """),""" +
                LDSMAINFIELD  + """(""" + NAME + """) from """ + LDS + """;
            """)
        connection.commit()
        
        # We fill the LDS_FIELDS
        ldss = LEDDBOps.getColValues(LEDDBOps.select(connection, LDSMETA, columnNames = [LDSMETA+ID]))
        
        for lds in ldss:
            cursor.execute('SELECT ' + FIELD + ' FROM ' + LDSB + ' WHERE ' + LDS + ' = %s ORDER BY ' +  BEAMINDEX,[lds,])
            if cursor.rowcount:
                fields = sorted(LEDDBOps.getColValues(cursor.fetchall()))
                for i in range(len(fields)): fields[i] = str(fields[i])
                LEDDBOps.update(connection, LDSMETA, {LDSFIELDS:','.join(fields)}, {LDSMETA+ID:lds})
        connection.commit()
        
        # Make the Temporal Joins 
        for table in [LDSJOINED, LDSBJOINED, LDSBPJOINED, MSJOINED]:
            try:
                if drop:
                    cursor.execute("""DROP TABLE """ + table)
                else:
                    cursor.execute("""DELETE FROM """ + table)
                connection.commit()
            except:
                connection = Connector(dbname, dbuser, dbhost).getConnection()
                cursor = connection.cursor()
        
        
        if drop:
            cursor.execute("CREATE TABLE " + LDSJOINED + " AS SELECT * FROM " + LDS +" LEFT JOIN " + LDSMETA + " ON (" + LDS + "." + NAME + "=" + LDSMETA + "." + LDSMETA+ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + NAME + "_idx ON " + LDSJOINED + "(" + NAME + ")")
            cursor.execute("GRANT SELECT ON " + LDSJOINED + " TO public")
        else:
            cursor.execute("INSERT INTO " + LDSJOINED + " SELECT * FROM " + LDS +" LEFT JOIN " + LDSMETA + " ON (" + LDS + "." + NAME + "=" + LDSMETA + "." + LDSMETA+ID + ")")
            
        connection.commit()
        
        if drop:
            cursor.execute("CREATE TABLE " + LDSBJOINED + " AS SELECT * FROM " + LDSB +" LEFT JOIN " + LDSBMETA + " ON (" + LDSB + "." + LDSB+ID + "=" + LDSBMETA + "." + LDSBMETA+ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + LDSB + ID + "_idx ON " + LDSBJOINED + "(" + LDSB + ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + LDSB + "_key ON " + LDSBJOINED + "(" + LDS +"," + BEAMINDEX +")")
            cursor.execute("GRANT SELECT ON " + LDSBJOINED + " TO public")
        else:
            cursor.execute("INSERT INTO " + LDSBJOINED + " SELECT * FROM " + LDSB +" LEFT JOIN " + LDSBMETA + " ON (" + LDSB + "." + LDSB+ID + "=" + LDSBMETA + "." + LDSBMETA+ID + ")")
        connection.commit()
        
        if drop:
            cursor.execute("CREATE TABLE " + LDSBPJOINED + " AS SELECT * FROM " + LDSBP +" LEFT JOIN " + LDSBPMETA + " ON (" + LDSBP + "." + LDSBP+ID + "=" + LDSBPMETA + "." + LDSBPMETA+ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + LDSBP + ID + "_idx ON " + LDSBPJOINED + "(" + LDSBP + ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + LDSBP + "_key ON " + LDSBPJOINED + "(" + LDSB + ID + "," + STORE + "," + INTTIME + "," + NUMCHAN + "," + VERSION +"," + RAW +"," + TAR +"," + BVF +")")
            cursor.execute("GRANT SELECT ON " + LDSBPJOINED + " TO public")
        else:
            cursor.execute("INSERT INTO " + LDSBPJOINED + " SELECT * FROM " + LDSBP +" LEFT JOIN " + LDSBPMETA + " ON (" + LDSBP + "." + LDSBP+ID + "=" + LDSBPMETA + "." + LDSBPMETA+ID + ")")
        connection.commit()
        
        if drop:
            cursor.execute("CREATE TABLE " + MSJOINED + " AS SELECT * FROM " + MS +" LEFT JOIN " + MSMETA + " ON (" + MS + "." + MS+ID + "=" + MSMETA + "." + MSMETA+ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + MS + ID + "_idx ON " + MSJOINED + "(" + MS + ID + ")")
            cursor.execute("CREATE UNIQUE INDEX " + MS + "_key ON " + MSJOINED + "(" + LDSBP + ID + "," + SBINDEX +")")
            cursor.execute("GRANT SELECT ON " + MSJOINED + " TO public")
        else:
            cursor.execute("INSERT INTO " + MSJOINED + " SELECT * FROM " + MS +" LEFT JOIN " + MSMETA + " ON (" + MS + "." + MS+ID + "=" + MSMETA + "." + MSMETA+ID + ")")
        connection.commit()
        
        cursor.close()
        connection.close()
        
        if not nottime:
            print 'Finished in ' + str(int(time.time()-tstart)) + ' seconds.'