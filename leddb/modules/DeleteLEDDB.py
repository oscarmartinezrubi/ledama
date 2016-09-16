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

class DeleteLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        options.add('query','q','Print the commands without executing them?',default=False)     
        
        # the information
        information = 'delete the LEDDB'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, showTimeInfo = False)   
    
    def execute(self, command):
        if self.query:
            print command + ';'
        else:
            self.cursor.execute( command)
    
    def process(self, dbname, dbuser, dbhost, query):
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        self.query = query
        
        # Get the cursor
        self.cursor = connection.cursor()
        
        # Delete the META tables and the temporal
        for table in (MSMETA, LDSBPMETA, LDSBMETA, LDSMETA, LDSJOINED, LDSBJOINED, LDSBPJOINED, MSJOINED):
            try:
                self.execute("""DROP TABLE """ + table)
                connection.commit()
            except:
                connection = Connector(dbname, dbuser, dbhost).getConnection()
                self.cursor = connection.cursor()

        # Delete the MOVIE tables
        self.execute("""DROP TABLE """ + GAINMOVIEHASSTATION + """,""" + GAINMOVIEHASMS + """,""" + GAINMOVIE + """ CASCADE""")
        self.execute("""DROP SEQUENCE """ + GAINMOVIE + SEQ + """ CASCADE""")
                
        # Delete the APP tables
        self.execute("""DROP TABLE """ + APPFILE + """,""" + APPRUN + """ CASCADE""")
        self.execute("""DROP SEQUENCE """ + APPRUN + SEQ +   """,""" + APPFILE + SEQ + """ CASCADE""")
        
        for diagTable in (GAIN, QTSTAT, QBSTAT, QFSTAT):
            self.cursor.execute('select tablename from pg_tables where tablename ~ %s order by tablename', [diagTable.lower()+"_part_*",])
            for (partName,) in self.cursor.fetchall():
                self.execute("""DROP TABLE """ + partName + """ CASCADE""")
        
        # Delete the DIAG tables
        self.execute("""DROP TABLE """ + QTSTAT + """,""" + QFSTAT + """,""" + QBSTAT + """,""" + QKIND + """,""" + GAIN + """ CASCADE""")
        self.execute("""DROP SEQUENCE """ + QTSTAT + SEQ +   """,""" + QFSTAT + SEQ + """,""" + QBSTAT + SEQ + """,""" + QKIND + SEQ + """,""" + GAIN + SEQ + """ CASCADE""")
        
        # Delete the REF tables
        self.execute("""DROP TABLE """ + MSP +  """,""" + PARENTPATH +  """,""" + HOST +  """,""" + MS + """,""" + LDSBP + """,""" + STORE + """,""" +  LDSB + """,""" + FIELD + """,""" + LDSHASSTATION +  """,""" + BASELINE +  """,""" + STATION +  """,""" + LDS +  """,""" + ANTTYPE +  """,""" + PROJECT + """ CASCADE""")
        self.execute("""DROP SEQUENCE """ + MSP+ SEQ +   """,""" + MS + SEQ + """,""" + LDSBP + SEQ  + """,""" + LDSB + SEQ + """,""" + BASELINE + SEQ + """ CASCADE""")
        
        # Delete the META-FUNCTIONS
        self.execute("""DROP FUNCTION """ + MSHASMSP +  """(integer)""")        
        self.execute("""DROP FUNCTION """ + MSHASGAIN +  """(integer)""")
        self.execute("""DROP FUNCTION """ + MSHASQUALITY  +  """(integer)""")
        self.execute("""DROP FUNCTION """ + MSHASGAINMOVIE  +  """(integer)""")
        
        self.execute("""DROP FUNCTION """ + LDSBPNUMMSP +  """(in integer, out f1 integer)""")    
        self.execute("""DROP FUNCTION """ + LDSBPTOTALSIZE +  """(in integer, out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSBPLASTMODIFICATION +  """(in integer, out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSBPMINCENTFREQ +  """(integer, out f1 double precision)""")
        self.execute("""DROP FUNCTION """ + LDSBPMAXCENTFREQ +  """(integer, out f1 double precision)""")
        self.execute("""DROP FUNCTION """ + LDSBPHASGAIN +  """(integer)""")
        self.execute("""DROP FUNCTION """ + LDSBPHASQUALITY  +  """(integer)""")
        self.execute("""DROP FUNCTION """ + LDSBPHASGAINMOVIE  +  """(integer)""")          
        
        self.execute("""DROP FUNCTION """ + LDSBNUMSB +  """(in integer, out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSBNUMLDSBP +  """(in integer, out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSBNUMMSP +  """(in integer, out f1 integer)""")                
        self.execute("""DROP FUNCTION """ + LDSBTOTALSIZE +  """(in integer, out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSBMINCENTFREQ +  """(in integer, out f1 double precision)""")
        self.execute("""DROP FUNCTION """ + LDSBMAXCENTFREQ +  """(in integer, out f1 double precision)""")  
        self.execute("""DROP FUNCTION """ + LDSBHASGAIN +  """(integer)""")
        self.execute("""DROP FUNCTION """ + LDSBHASQUALITY  +  """(integer)""")
        self.execute("""DROP FUNCTION """ + LDSBHASGAINMOVIE  +  """(integer)""")
        
        self.execute("""DROP FUNCTION """ + LDSNUMBEAMS +  """(in varchar(6), out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSMAINFIELD +  """(in varchar(6), out f1 text)""")
        self.execute("""DROP FUNCTION """ + LDSDURATION +  """(in varchar(6), out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSNUMSTATIONS +  """(in varchar(6), out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSNUMSB +  """(in varchar(6), out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSNUMMSP +  """(in varchar(6), out f1 integer)""")                
        self.execute("""DROP FUNCTION """ + LDSTOTALSIZE +  """(in varchar(6), out f1 integer)""")
        self.execute("""DROP FUNCTION """ + LDSMINCENTFREQ +  """(in varchar(6), out f1 double precision)""")
        self.execute("""DROP FUNCTION """ + LDSMAXCENTFREQ +  """(in varchar(6), out f1 double precision)""")  
        self.execute("""DROP FUNCTION """ + LDSHASGAIN +  """(varchar(6))""")
        self.execute("""DROP FUNCTION """ + LDSHASQUALITY  +  """(varchar(6))""")
        self.execute("""DROP FUNCTION """ + LDSHASGAINMOVIE  +  """(varchar(6))""")
        
        # Commit the changes
        connection.commit()
        self.cursor.close()
        connection.close()