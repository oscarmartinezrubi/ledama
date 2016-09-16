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

class CreateLEDDB(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        options.add('functions','f','Create only the functions?',default=False)               
        options.add('query','q','Print the commands without executing them?',default=False)
        
        # the information
        information = 'create a LEDDB'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, showTimeInfo = False)   
        
    def execute(self, command):
        if self.query:
            print command + ';'
        else:
            self.cursor.execute( command)
    
    def process(self, dbname, dbuser, dbhost, functions, query):
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        self.query = query
        
        # Get the cursor
        self.cursor = connection.cursor()
        
        # ddd   ddd dddd  
        # d  d  d   d      
        # d  d  d   d      
        # ddd   ddd dddd
        # d d   d   d   
        # d  d  d   d   
        # dd  d ddd d   
        
        if not functions:
            # Create Project Table
            self.execute( """CREATE TABLE """ + PROJECT + """ (
                """ + NAME + """ VARCHAR(40) PRIMARY KEY
            )""")
            self.execute( "GRANT SELECT ON " + PROJECT + " TO public")
            
            # Create AntennaType Table
            self.execute( """CREATE TABLE """ + ANTTYPE + """ (
                """ + NAME + """ VARCHAR(10) PRIMARY KEY
            )""")
            self.execute( "GRANT SELECT ON " + ANTTYPE + " TO public")
    
            # Create LOFARDataSet Table
            self.execute( """CREATE TABLE """ + LDS + """ (
                """ + NAME + """ VARCHAR(15) PRIMARY KEY,
                """ + PROJECT + """ VARCHAR(40) REFERENCES """ + PROJECT + """ (""" + NAME + """),
                """ + ANTTYPE + """ VARCHAR(10) REFERENCES """ + ANTTYPE + """ (""" + NAME + """),
                """ + INITIALMJD + """ DOUBLE PRECISION,
                """ + FINALMJD + """ DOUBLE PRECISION,
                """ + INITIALUTC + """ VARCHAR(25),
                """ + FINALUTC + """ VARCHAR(25)
            )""")
            self.execute( "GRANT SELECT ON " + LDS + " TO public")
            # We select by Name, and if we have more data that the stored one we update it
            
            # Create Station Table
            self.execute( """CREATE TABLE """ + STATION + """ (
                """ + NAME + """ VARCHAR(20) PRIMARY KEY,
                """ + ANTTYPE + """ VARCHAR(10) NOT NULL REFERENCES """ + ANTTYPE + """ (""" + NAME + """),
                """ + LOCATIONTYPE + """ VARCHAR(2) NOT NULL,
                """ + POSITION + """ DOUBLE PRECISION[3] NOT NULL
            )""")
            self.execute( "GRANT SELECT ON " + STATION + " TO public")
            
            # Create LOFARDataSetHasStation Table
            self.execute( """CREATE TABLE """ + LDSHASSTATION + """ (
                """ + LDS + """ VARCHAR(15) REFERENCES """ + LDS + """ (""" + NAME + """),
                """ + STATION + """ VARCHAR(20) REFERENCES """ + STATION + """ (""" + NAME + """),
                PRIMARY KEY (""" + LDS + """, """ + STATION + """)
            )""")
            self.execute( "GRANT SELECT ON " + LDSHASSTATION + " TO public")
            
            # Create BASELINE Table
            self.execute( """CREATE SEQUENCE """ + BASELINE + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + BASELINE + """ (
                """ + BASELINE + ID + """ INTEGER PRIMARY KEY DEFAULT nextval('""" + BASELINE + SEQ + """'),
                """ + STATION1 + """ VARCHAR(20) REFERENCES """ + STATION + """ (""" + NAME + """),
                """ + STATION2 + """ VARCHAR(20) REFERENCES """ + STATION + """ (""" + NAME + """),
                UNIQUE(""" + STATION1 +""",""" + STATION2 +"""),
                CHECK (""" + STATION2 + """ >= """ + STATION1 + """)
            )""")
            self.execute( "GRANT SELECT ON " + BASELINE + " TO public")
            
            # Create Field Table
            self.execute( """CREATE TABLE """ + FIELD + """ (
                """ + NAME + """ TEXT PRIMARY KEY
            )""")
            self.execute( "GRANT SELECT ON " + FIELD + " TO public")
            
            #Create LDSB
            self.execute( """CREATE SEQUENCE """ + LDSB + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + LDSB + """ (
                """ + LDSB + ID + """ INTEGER PRIMARY KEY DEFAULT nextval('""" + LDSB + SEQ + """'),
                """ + LDS + """ VARCHAR(15) NOT NULL REFERENCES """ + LDS + """ (""" + NAME + """),
                """ + BEAMINDEX + """ INTEGER NOT NULL,
                UNIQUE(""" + LDS +""",""" + BEAMINDEX +"""),
                """ + FIELD + """ TEXT REFERENCES """ + FIELD + """ (""" + NAME + """),
                """ + PHASEDIRRA + """ DOUBLE PRECISION,
                """ + PHASEDIRDEC + """ DOUBLE PRECISION
            )""")
            self.execute( "GRANT SELECT ON " + LDSB + " TO public")
            
            
            # Create STORE Table
            self.execute( """CREATE TABLE """ + STORE + """ (
                """ + NAME + """ VARCHAR(20) PRIMARY KEY
            )""")
            self.execute( "GRANT SELECT ON " + STORE + " TO public")
            
            # Create LOFARDataSetBEAMProduct Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + LDSBP + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + LDSBP + """ (
                """ + LDSBP + ID + """ INTEGER PRIMARY KEY DEFAULT nextval('""" + LDSBP + SEQ + """'),
                """ + LDSB + ID + """ INTEGER NOT NULL REFERENCES """ + LDSB + """ (""" + LDSB + ID + """),
                """ + STORE + """ VARCHAR(20) NOT NULL REFERENCES """ + STORE + """ (""" + NAME + """),
                """ + INTTIME + """ DOUBLE PRECISION NOT NULL,
                """ + NUMCHAN + """ INTEGER NOT NULL,
                """ + VERSION + """ INTEGER NOT NULL,
                """ + RAW + """ BOOLEAN NOT NULL,
                """ + TAR + """ BOOLEAN NOT NULL,
                """ + BVF + """ BOOLEAN NOT NULL,
                UNIQUE(""" + LDSB + ID + """,""" + STORE + """,""" + INTTIME + """,""" + NUMCHAN + """,""" + VERSION +""",""" + RAW +""",""" + TAR +""",""" + BVF +"""),
                """ + DESCR + """ TEXT,
                """ + MAINUSER + """ VARCHAR(20),
                """ + ADDDATE + """ INTEGER,
                """ + FLAGGED + """ BOOLEAN,
                """ + AVERAGED + """ BOOLEAN,
                """ + CALIBRATED + """ BOOLEAN,
                """ + DIRDEPCAL + """ BOOLEAN,
                """ + DIRINDEPCAL + """ BOOLEAN
            )""")
            self.execute( "GRANT SELECT ON " + LDSBP + " TO public")
            
            # Create MS Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + MS + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + MS + """ (
                """ + MS + ID + """ INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('""" + MS + SEQ + """'),
                """ + LDSBP + ID + """ INTEGER NOT NULL REFERENCES """ + LDSBP + """ (""" + LDSBP + ID + """),
                """ + SBINDEX + """ INTEGER NOT NULL,
                UNIQUE(""" + LDSBP + ID + """,""" + SBINDEX + """),
                """ + CENTFREQ + """ DOUBLE PRECISION,
                """ + BW + """ DOUBLE PRECISION
            )""")
            self.execute( "GRANT SELECT ON " + MS + " TO public")
            
            # Create Host Table
            self.execute( """CREATE TABLE """ + HOST + """ (
                """ + NAME + """ VARCHAR(20) PRIMARY KEY,
                """ + REQUIRESUPDATE + """ BOOLEAN DEFAULT FALSE,
                """ + USAGEMON + """ TEXT,
                """ + GPUUSAGEMON + """ TEXT,
                """ + STORAGEMON + """ TEXT,
                """ + NETMON + """ TEXT
            )""")
            self.execute( "GRANT SELECT ON " + HOST + " TO public")
            
            
            # Create Location Table
            self.execute( """CREATE TABLE """ + PARENTPATH + """ (
                """ + NAME + """ TEXT PRIMARY KEY
            )""")
            self.execute( "GRANT SELECT ON " + PARENTPATH + " TO public")
            
            # Create MeasurementSetProduct Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + MSP + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + MSP + """ (
                """ + MSP + ID + """ INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('""" + MSP + SEQ + """'),
                """ + MS + ID + """ INTEGER NOT NULL REFERENCES """ + MS + """ (""" + MS + ID + """),
                """ + HOST + """ VARCHAR(20) NOT NULL REFERENCES """ + HOST + """ (""" + NAME + """),
                """ + PARENTPATH + """ TEXT NOT NULL REFERENCES """ + PARENTPATH + """ (""" + NAME + """),
                """ + NAME + """ VARCHAR(100) NOT NULL,
                UNIQUE(""" + HOST + """,""" + PARENTPATH + """,""" + NAME + """),
                """ + LASTCHECK + """ INTEGER,
                """ + LASTMODIFICATION + """ INTEGER,
                """ + SIZE + """ INTEGER
            )""")
            self.execute( "GRANT SELECT ON " + MSP + " TO public")
            self.execute( "CREATE INDEX " + MSP + "_ms_id_index ON " + MSP + "(" + MS + ID + ")")
            
            #  ddd   ddddd ddddd
            # d   d  d   d d   d
            # d   d  d   d d   d
            # ddddd  dddd  dddd
            # d   d  d     d
            # d   d  d     d
            # d   d  d     d 
            
            # Create APPRUN Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + APPRUN + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + APPRUN + """ (
                """ + APPRUN + ID + """ INTEGER PRIMARY KEY DEFAULT nextval('""" + APPRUN + SEQ + """'),
                """ + LDSBP + ID + """ INTEGER NOT NULL REFERENCES """ + LDSBP + """ (""" + LDSBP + ID + """),
                """ + ORDERINDEX + """ INTEGER NOT NULL,
                UNIQUE(""" + LDSBP + ID + """,""" + ORDERINDEX +"""),
                """ + APPNAME + """ VARCHAR(20),
                """ + DESCR + """ TEXT
            )""")
            self.execute( "GRANT SELECT ON " + APPRUN + " TO public")
    
            # Create APPFILE Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + APPFILE + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + APPFILE + """ (
                """ + APPFILE + ID + """ INTEGER PRIMARY KEY DEFAULT nextval('""" + APPFILE + SEQ + """'),
                """ + APPRUN + ID + """ INTEGER NOT NULL REFERENCES """ + APPRUN + """ (""" + APPRUN + ID + """),
                """ + FILEPATH + """ TEXT
            )""")
            self.execute( "GRANT SELECT ON " + APPFILE + " TO public")
            
            # ddd  ddd   ddd   dddd  
            # d  d  d   d   d  d   
            # d  d  d   d   d  d   
            # d  d  d   ddddd  d  dd
            # d  d  d   d   d  d   d
            # d  d  d   d   d  d   d
            # ddd   d   d   d   dddd
    
            # Create GAIN TABLE Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + GAIN + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + GAIN + """ (
                """ + GAIN + ID + """ INTEGER NOT NULL DEFAULT nextval('""" + GAIN + SEQ + """'),
                """ + MS + ID + """ INTEGER NOT NULL,
                """ + STATION + """ VARCHAR(20) NOT NULL,
                """ + DIRRA + """ DOUBLE PRECISION NOT NULL,
                """ + DIRDEC + """ DOUBLE PRECISION NOT NULL,
                """ + FSTEP + """ DOUBLE PRECISION,
                """ + TSTEP + """ DOUBLE PRECISION,
                """ + LASTCHECK + """ INTEGER,
                """ + VALUES + """ DOUBLE PRECISION[][][][]
            )""")
            self.execute( "GRANT SELECT ON " + GAIN + " TO public")
            
            # Create QualityKind Table
            self.execute( """CREATE SEQUENCE """ + QKIND + SEQ + """ MINVALUE 1 INCREMENT 1 START 1""")
            self.execute( """CREATE TABLE """ + QKIND + """ (
                """ + QKIND + ID + """ INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('""" + QKIND + SEQ + """'),
                """ + NAME + """ VARCHAR(30) NOT NULL UNIQUE,
                """ + DESCR + """ TEXT
            )""")
            self.execute( "GRANT SELECT ON " + QKIND + " TO public")
            
            # Create QualityTimeStat Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + QTSTAT + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + QTSTAT + """ (
                """ + QTSTAT + ID + """ INTEGER NOT NULL DEFAULT nextval('""" + QTSTAT + SEQ + """'),
                """ + MS + ID + """ INTEGER NOT NULL,
                """ + QKIND + ID + """ INTEGER NOT NULL,
                """ + FSTEP + """ DOUBLE PRECISION,
                """ + TSTEP + """ DOUBLE PRECISION,
                """ + LASTCHECK + """ INTEGER,
                """ + VALUES + """ DOUBLE PRECISION[][][][]
            )""")
            self.execute( "GRANT SELECT ON " + QTSTAT + " TO public")
            
            # Create QualityFrequencyStat Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + QFSTAT + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + QFSTAT + """ (
                """ + QFSTAT + ID + """ INTEGER NOT NULL DEFAULT nextval('""" + QFSTAT + SEQ + """'),
                """ + MS + ID + """ INTEGER NOT NULL,
                """ + QKIND + ID + """ INTEGER NOT NULL,
                """ + FSTEP + """ DOUBLE PRECISION,
                """ + LASTCHECK + """ INTEGER,
                """ + VALUES + """ DOUBLE PRECISION[][][]
            )""")
            self.execute( "GRANT SELECT ON " + QFSTAT + " TO public")
            
            # Create QualityBaselineStat Table
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + QBSTAT + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + QBSTAT + """ (
                """ + QBSTAT + ID + """ BIGINT NOT NULL DEFAULT nextval('""" + QBSTAT + SEQ + """'),
                """ + MS + ID + """ INTEGER NOT NULL,
                """ + QKIND + ID + """ INTEGER NOT NULL,
                """ + BASELINE + ID + """ INTEGER NOT NULL,
                """ + FSTEP + """ DOUBLE PRECISION,       
                """ + LASTCHECK + """ INTEGER,
                """ + VALUES + """ DOUBLE PRECISION[][][]
            )""")
            self.execute( "GRANT SELECT ON " + QBSTAT + " TO public")
    
            # m   m oooo v   v
            # mm mm o  o v   v
            # m m m o  o  v v
            # m   m o  o  v v
            # m   m o  o  v v
            # m   m o  o   v
            # m   m oooo   v
            # Create the GAIN Movie Table 
            # For it we create the sequence for assigning the ids
            self.execute( """CREATE SEQUENCE """ + GAINMOVIE + SEQ + """ MINVALUE 0 INCREMENT 1 START 0""")
            self.execute( """CREATE TABLE """ + GAINMOVIE + """ (
                """ + GAINMOVIE + ID + """ INTEGER NOT NULL PRIMARY KEY DEFAULT nextval('""" + GAINMOVIE + SEQ + """'),
                """ + FILEPATH + """ TEXT NOT NULL,
                """ + HOST + """ VARCHAR(20) NOT NULL REFERENCES """ + HOST + """ (""" + NAME + """),
                UNIQUE(""" + FILEPATH + """,""" + HOST + """),
                """ + SIZE + """ INTEGER NOT NULL,
                """ + XAXIS + """ VARCHAR(4) NOT NULL,
                """ + JONES + """ INTEGER[] NOT NULL,
                """ + POLAR + """ BOOLEAN NOT NULL,
                """ + REFSTATION + """ VARCHAR(20) NOT NULL REFERENCES """ + STATION + """ (""" + NAME + """),
                """ + TIMES + """ INTEGER[3] NOT NULL,
                """ + CHANNELS + """ INTEGER[3] NOT NULL,
                """ + YRANGE + """ TEXT NOT NULL, 
                """ + MESSAGE + """ TEXT,
                """ + LASTCHECK + """ INTEGER
            )""")
            self.execute( "GRANT SELECT ON " + GAINMOVIE + " TO public")
            
            # Create GAIN_MOVIE_HAS_MSs Table
            self.execute( """CREATE TABLE """ + GAINMOVIEHASMS + """ (
                """ + GAINMOVIE + ID + """ INTEGER REFERENCES """ + GAINMOVIE + """ (""" + GAINMOVIE + ID + """),
                """ + MS + ID + """ INTEGER REFERENCES """ + MS + """ (""" + MS + ID + """),
                PRIMARY KEY (""" + GAINMOVIE + ID + """, """ + MS + ID + """)
            )""")
            self.execute( "GRANT SELECT ON " + GAINMOVIEHASMS + " TO public") 
            
            # Create GAIN_MOVIE_HAS_MSs Table
            self.execute( """CREATE TABLE """ + GAINMOVIEHASSTATION + """ (
                """ + GAINMOVIE + ID + """ INTEGER REFERENCES """ + GAINMOVIE + """ (""" + GAINMOVIE + ID + """),
                """ + STATION + """ VARCHAR(20) REFERENCES """ + STATION + """ (""" + NAME + """),
                """ + DELAY + """ DOUBLE PRECISION,
                PRIMARY KEY (""" + GAINMOVIE + ID + """, """ + STATION + """)
            )""")
            self.execute( "GRANT SELECT ON " + GAINMOVIEHASSTATION + " TO public")
            
            # d   d dddd ddddd  ddd  
            # dd dd d      d   d   d     
            # d d d d      d   d   d   
            # d   d ddd    d   ddddd
            # d   d d      d   d   d
            # d   d d      d   d   d
            # d   d dddd   d   d   d
    
            # MS META
            self.execute( """CREATE TABLE """ + MSMETA + """ (
                """ + MSMETA + ID + """ INTEGER NOT NULL PRIMARY KEY REFERENCES """ + MS + """ (""" + MS + ID + """),
                """ + MSHASMSP + """ BOOLEAN,
                """ + MSHASGAIN + """ BOOLEAN,
                """ + MSHASQUALITY + """ BOOLEAN,
                """ + MSHASGAINMOVIE + """ BOOLEAN
            )""")
            self.execute( "GRANT SELECT ON " + MSMETA + " TO public")
            # LDSBP META 
            self.execute( """CREATE TABLE """ + LDSBPMETA + """ (
                """ + LDSBPMETA + ID + """ INTEGER NOT NULL PRIMARY KEY REFERENCES """ + LDSBP + """ (""" + LDSBP + ID + """),
                """ + LDSBPMINCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSBPMAXCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSBPNUMMSP + """ INTEGER,
                """ + LDSBPTOTALSIZE + """ INTEGER,
                """ + LDSBPLASTMODIFICATION + """ INTEGER,
                """ + LDSBPHASGAIN + """ BOOLEAN,
                """ + LDSBPHASQUALITY + """ BOOLEAN,
                """ + LDSBPHASGAINMOVIE + """ BOOLEAN
            )""")
            self.execute( "GRANT SELECT ON " + LDSBPMETA + " TO public")
            # LDSB META 
            self.execute( """CREATE TABLE """ + LDSBMETA + """ (
                """ + LDSBMETA + ID + """ INTEGER NOT NULL PRIMARY KEY REFERENCES """ + LDSB + """ (""" + LDSB + ID + """),
                """ + LDSBNUMSB + """ INTEGER,
                """ + LDSBNUMLDSBP + """ INTEGER,
                """ + LDSBMINCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSBMAXCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSBNUMMSP + """ INTEGER,
                """ + LDSBTOTALSIZE + """ INTEGER,
                """ + LDSBHASGAIN + """ BOOLEAN,
                """ + LDSBHASQUALITY + """ BOOLEAN,
                """ + LDSBHASGAINMOVIE + """ BOOLEAN
            )""")
            self.execute( "GRANT SELECT ON " + LDSBMETA + " TO public")
            # LDS META 
            self.execute( """CREATE TABLE """ + LDSMETA + """ (
                """ + LDSMETA+ID + """ VARCHAR(15) NOT NULL PRIMARY KEY REFERENCES """ + LDS + """ (""" + NAME + """),
                """ + LDSNUMBEAMS + """ INTEGER,
                """ + LDSDURATION + """ INTEGER,
                """ + LDSNUMSTATIONS + """ INTEGER,
                """ + LDSNUMSB + """ INTEGER,
                """ + LDSMINCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSMAXCENTFREQ + """ DOUBLE PRECISION,
                """ + LDSNUMMSP + """ INTEGER,
                """ + LDSTOTALSIZE + """ INTEGER,
                """ + LDSHASGAIN + """ BOOLEAN,
                """ + LDSHASQUALITY + """ BOOLEAN,
                """ + LDSHASGAINMOVIE + """ BOOLEAN,
                """ + LDSMAINFIELD + """ TEXT,
                """ + LDSFIELDS + """ TEXT
            )""")
            self.execute( "GRANT SELECT ON " + LDSMETA + " TO public")

        # Create the METADATA Functions
        # Get true if there is some MSP related to given MS id
        self.execute( """CREATE FUNCTION """ + MSHASMSP +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + MSP + """.""" + MSP+ID + """ from """ + MSP + """ where """ + MSP + """.""" + MS+ID  + """ = $1 LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + MSHASGAIN +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + GAIN + """.""" + GAIN+ID + """ from """ + GAIN + """ where """ + GAIN + """.""" + MS+ID  + """ = $1 LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + MSHASQUALITY +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + QFSTAT + """.""" + QFSTAT+ID + """ from """ + QFSTAT + """ where """ + QFSTAT + """.""" + MS+ID  + """ = $1 LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + MSHASGAINMOVIE +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + GAINMOVIEHASMS + """.""" + GAINMOVIE+ID + """ from """ + GAINMOVIEHASMS + """ where """ + GAINMOVIEHASMS + """.""" + MS+ID  + """ = $1 LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        
        
        # For a certain LDSBP get the number of measurement sets products
        self.execute( """CREATE FUNCTION """ + LDSBPNUMMSP +  """(in integer, out f1 integer)
            AS $$SELECT CAST(count(""" + MSP + """.""" + MSP+ID + """) AS INTEGER) FROM """ + MSP + """,""" + MS + """ WHERE """ + MSP + """.""" + MS+ID  + """ = """ +  MS + """.""" + MS+ID + """ AND """ + MS + """.""" + LDSBP+ID  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")    
        self.execute( """CREATE FUNCTION """ + LDSBPTOTALSIZE +  """(in integer, out f1 integer)
            AS $$SELECT CAST(sum(""" + MSP + """.""" + SIZE + """) AS INTEGER) FROM """ + MSP + """,""" + MS + """ WHERE """ + MSP + """.""" + MS+ID  + """ = """ +  MS + """.""" + MS+ID + """ AND """ + MS + """.""" + LDSBP+ID  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBPLASTMODIFICATION +  """(in integer, out f1 integer)
            AS $$SELECT CAST(max(""" + MSP + """.""" + LASTMODIFICATION + """) AS INTEGER) FROM """ + MSP + """,""" + MS + """ WHERE """ + MSP + """.""" + MS+ID  + """ = """ +  MS + """.""" + MS+ID + """ AND """ + MS + """.""" + LDSBP+ID  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        # Get the minimum CENTRAL FREQUENCY of the MS related to the LDSBP Id
        self.execute( """CREATE FUNCTION """ + LDSBPMINCENTFREQ +  """(integer, out f1 double precision)
            AS 'SELECT min(""" + MS + """.""" + CENTFREQ + """) FROM """ + MS + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBPMAXCENTFREQ +  """(integer, out f1 double precision)
            AS 'SELECT max(""" + MS + """.""" + CENTFREQ + """) FROM """ + MS + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        # Uses the MSMETA to get information of the LDSBP
        self.execute( """CREATE FUNCTION """ + LDSBPHASGAIN +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + MSMETA + """.""" + MSMETA+ID + """ FROM """ + MS + """,""" + MSMETA + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = $1 AND """ +  MS + """.""" + MS+ID + """ = """ + MSMETA + """.""" + MSMETA+ID  + """ AND """ + MSMETA + """.""" + MSHASGAIN + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")   
        self.execute( """CREATE FUNCTION """ + LDSBPHASQUALITY +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + MSMETA + """.""" + MSMETA+ID + """ FROM """ + MS + """,""" + MSMETA + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = $1 AND """ +  MS + """.""" + MS+ID + """ = """ + MSMETA + """.""" + MSMETA+ID  + """ AND """ + MSMETA + """.""" + MSHASQUALITY + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")           
        self.execute( """CREATE FUNCTION """ + LDSBPHASGAINMOVIE +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + MSMETA + """.""" + MSMETA+ID + """ FROM """ + MS + """,""" + MSMETA + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = $1 AND """ +  MS + """.""" + MS+ID + """ = """ + MSMETA + """.""" + MSMETA+ID  + """ AND """ + MSMETA + """.""" + MSHASGAINMOVIE + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")   
            
        self.execute( """CREATE FUNCTION """ + LDSBNUMSB +  """(in integer, out f1 integer)
            AS $$SELECT CAST(count(*) AS INTEGER) FROM (SELECT DISTINCT """ + MS + """.""" + SBINDEX + """ FROM """ + MS + """,""" + LDSBP + """ WHERE """ + MS + """.""" + LDSBP+ID  + """ = """ +  LDSBP + """.""" + LDSBP+ID + """ AND """ + LDSBP + """.""" + LDSB+ID   + """ = $1) AS a;$$
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBNUMLDSBP +  """(in integer, out f1 integer)
            AS $$SELECT CAST(count(*) AS INTEGER) FROM (SELECT """ + LDSBP + """.""" + LDSBP + ID + """ FROM """ + LDSBP + """ WHERE """ + LDSBP + """.""" + LDSB+ID   + """ = $1) AS a;$$
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBNUMMSP +  """(in integer, out f1 integer)
            AS $$SELECT CAST(sum(""" + LDSBPMETA + """.""" + LDSBPNUMMSP + """) AS INTEGER) FROM """ + LDSBPMETA + """,""" + LDSBP + """ WHERE """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ = """ +  LDSBP + """.""" + LDSBP+ID + """ AND """ + LDSBP + """.""" + LDSB + ID + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")                
        self.execute( """CREATE FUNCTION """ + LDSBTOTALSIZE +  """(in integer, out f1 integer)
            AS $$SELECT CAST(sum(""" + LDSBPMETA + """.""" + LDSBPTOTALSIZE + """) AS INTEGER) FROM """ + LDSBPMETA + """,""" + LDSBP + """ WHERE """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ = """ +  LDSBP + """.""" + LDSBP+ID + """ AND """ + LDSBP + """.""" + LDSB + ID  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBMINCENTFREQ +  """(in integer, out f1 double precision)
            AS 'SELECT min(""" + LDSBPMETA + """.""" + LDSBPMINCENTFREQ + """) FROM """ + LDSBPMETA + """,""" + LDSBP + """ WHERE """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ = """ +  LDSBP + """.""" + LDSBP+ID + """ AND """ + LDSBP + """.""" + LDSB + ID  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        # Get the maximum central frequency of the related SBs to a LDS        
        self.execute( """CREATE FUNCTION """ + LDSBMAXCENTFREQ +  """(in integer, out f1 double precision)
            AS 'SELECT max(""" + LDSBPMETA + """.""" + LDSBPMAXCENTFREQ + """) FROM """ + LDSBPMETA + """,""" + LDSBP + """ WHERE """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ = """ +  LDSBP + """.""" + LDSBP+ID + """ AND """ + LDSBP + """.""" + LDSB + ID  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")  
        self.execute( """CREATE FUNCTION """ + LDSBHASGAIN +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBPMETA + """.""" + LDSBPMETA+ID + """ FROM """ + LDSBP + """,""" + LDSBPMETA + """ WHERE """ + LDSBP + """.""" + LDSB + ID  + """ = $1 AND """ +  LDSBP + """.""" + LDSBP+ID + """ = """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ AND """ + LDSBPMETA + """.""" + LDSBPHASGAIN + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBHASQUALITY +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBPMETA + """.""" + LDSBPMETA+ID + """ FROM """ + LDSBP + """,""" + LDSBPMETA + """ WHERE """ + LDSBP + """.""" + LDSB + ID  + """ = $1 AND """ +  LDSBP + """.""" + LDSBP+ID + """ = """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ AND """ + LDSBPMETA + """.""" + LDSBPHASQUALITY + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSBHASGAINMOVIE +  """(integer) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBPMETA + """.""" + LDSBPMETA+ID + """ FROM """ + LDSBP + """,""" + LDSBPMETA + """ WHERE """ + LDSBP + """.""" + LDSB + ID  + """ = $1 AND """ +  LDSBP + """.""" + LDSBP+ID + """ = """ + LDSBPMETA + """.""" + LDSBPMETA+ID  + """ AND """ + LDSBPMETA + """.""" + LDSBPHASGAINMOVIE + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        
        self.execute( """CREATE FUNCTION """ + LDSNUMBEAMS +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(count(*) AS INTEGER) FROM (SELECT DISTINCT """ + LDSB + """.""" + BEAMINDEX + """ FROM """ + LDSB + """ WHERE """ +  LDSB + """.""" + LDS  + """ = $1) AS a;$$
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSDURATION +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(round(""" + FINALMJD + """ - """ + INITIALMJD + """) AS INTEGER) FROM """ + LDS + """ WHERE """  + NAME + """ = $1 ;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")    
        # Get the number of different SB indexes related to a certain LDS
        self.execute( """CREATE FUNCTION """ + LDSNUMSTATIONS +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(count(*) AS INTEGER) FROM """ + LDSHASSTATION + """ WHERE """  + LDS + """ = $1 ;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")    
        self.execute( """CREATE FUNCTION """ + LDSNUMSB +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(sum(""" + LDSBMETA + """.""" + LDSBNUMSB + """) AS INTEGER) FROM """ + LDSBMETA + """,""" + LDSB + """ WHERE """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ = """ +  LDSB + """.""" + LDSB+ID + """ AND """ + LDSB + """.""" + LDS  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")
        # For a certain LDS get the number of measurement sets products
        self.execute( """CREATE FUNCTION """ + LDSNUMMSP +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(sum(""" + LDSBMETA + """.""" + LDSBNUMMSP + """) AS INTEGER) FROM """ + LDSBMETA + """,""" + LDSB + """ WHERE """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ = """ +  LDSB + """.""" + LDSB+ID + """ AND """ + LDSB + """.""" + LDS  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")                
        self.execute( """CREATE FUNCTION """ + LDSTOTALSIZE +  """(in varchar(15), out f1 integer)
            AS $$SELECT CAST(sum(""" + LDSBMETA + """.""" + LDSBTOTALSIZE + """) AS INTEGER) FROM """ + LDSBMETA + """,""" + LDSB + """ WHERE """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ = """ +  LDSB + """.""" + LDSB+ID + """ AND """ + LDSB + """.""" + LDS  + """ = $1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")                 
        # Get the minimum central frequency of the related SBs to a LDS
        self.execute( """CREATE FUNCTION """ + LDSMINCENTFREQ +  """(in varchar(15), out f1 double precision)
            AS 'SELECT min(""" + LDSBMETA + """.""" + LDSBMINCENTFREQ + """) FROM """ + LDSBMETA + """,""" + LDSB + """ WHERE """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ = """ +  LDSB + """.""" + LDSB+ID + """ AND """ + LDSB + """.""" + LDS  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        # Get the maximum central frequency of the related SBs to a LDS        
        self.execute( """CREATE FUNCTION """ + LDSMAXCENTFREQ +  """(in varchar(15), out f1 double precision)
            AS 'SELECT max(""" + LDSBMETA + """.""" + LDSBMAXCENTFREQ + """) FROM """ + LDSBMETA + """,""" + LDSB + """ WHERE """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ = """ +  LDSB + """.""" + LDSB+ID + """ AND """ + LDSB + """.""" + LDS  + """ = $1;'
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")  

        self.execute( """CREATE FUNCTION """ + LDSHASGAIN +  """(varchar(15)) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBMETA + """.""" + LDSBMETA+ID + """ FROM """ + LDSB + """,""" + LDSBMETA + """ WHERE """ + LDSB + """.""" + LDS  + """ = $1 AND """ +  LDSB + """.""" + LDSB+ID + """ = """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ AND """ + LDSBMETA + """.""" + LDSBHASGAIN + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSHASQUALITY +  """(varchar(15)) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBMETA + """.""" + LDSBMETA+ID + """ FROM """ + LDSB + """,""" + LDSBMETA + """ WHERE """ + LDSB + """.""" + LDS  + """ = $1 AND """ +  LDSB + """.""" + LDSB+ID + """ = """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ AND """ + LDSBMETA + """.""" + LDSBHASQUALITY + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSHASGAINMOVIE +  """(varchar(15)) RETURNS BOOLEAN 
            AS $$ SELECT count(a)>0 FROM (select """ + LDSBMETA + """.""" + LDSBMETA+ID + """ FROM """ + LDSB + """,""" + LDSBMETA + """ WHERE """ + LDSB + """.""" + LDS  + """ = $1 AND """ +  LDSB + """.""" + LDSB+ID + """ = """ + LDSBMETA + """.""" + LDSBMETA+ID  + """ AND """ + LDSBMETA + """.""" + LDSBHASGAINMOVIE + """ = TRUE LIMIT 1) AS a;$$
            LANGUAGE SQL
            IMMUTABLE
            RETURNS NULL ON NULL INPUT""")
        self.execute( """CREATE FUNCTION """ + LDSMAINFIELD +  """(in varchar(15), out f1 text)
            AS $$SELECT """ + LDSB + """.""" + FIELD + """ FROM """ + LDSB + """ WHERE """ + LDSB + """.""" + LDS  + """ = $1 ORDER BY """ + LDSB + """.""" + BEAMINDEX + """ LIMIT 1;$$ 
            LANGUAGE SQL 
            IMMUTABLE 
            RETURNS NULL ON NULL INPUT""")     
        
        connection.commit()
        
        # JJJJ   OOO   I  N   N
        #    J  O   O  I  NN  N
        #    J  O   O  I  N N N
        #    J  O   O  I  N N N
        # J  J  O   O  I  N  NN
        # J  J  O   O  I  N  NN
        # JJJ    OOO   I  N   N
        
        if not functions:
        
            self.execute( "CREATE TABLE " + LDSJOINED + " AS SELECT * FROM " + LDS +" LEFT JOIN " + LDSMETA + " ON (" + LDS + "." + NAME + "=" + LDSMETA + "." + LDSMETA+ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + NAME + "_idx ON " + LDSJOINED + "(" + NAME + ")")
            self.execute( "GRANT SELECT ON " + LDSJOINED + " TO public")
            
            self.execute( "CREATE TABLE " + LDSBJOINED + " AS SELECT * FROM " + LDSB +" LEFT JOIN " + LDSBMETA + " ON (" + LDSB + "." + LDSB+ID + "=" + LDSBMETA + "." + LDSBMETA+ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + LDSB + ID + "_idx ON " + LDSBJOINED + "(" + LDSB + ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + LDSB + "_key ON " + LDSBJOINED + "(" + LDS +"," + BEAMINDEX +")")
            self.execute( "GRANT SELECT ON " + LDSBJOINED + " TO public")
            
            self.execute( "CREATE TABLE " + LDSBPJOINED + " AS SELECT * FROM " + LDSBP +" LEFT JOIN " + LDSBPMETA + " ON (" + LDSBP + "." + LDSBP+ID + "=" + LDSBPMETA + "." + LDSBPMETA+ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + LDSBP + ID + "_idx ON " + LDSBPJOINED + "(" + LDSBP + ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + LDSBP + "_key ON " + LDSBPJOINED + "(" + LDSB + ID + "," + STORE + "," + INTTIME + "," + NUMCHAN + "," + VERSION +"," + RAW +"," + TAR +"," + BVF +")")
            self.execute( "GRANT SELECT ON " + LDSBPJOINED + " TO public")
            
            self.execute( "CREATE TABLE " + MSJOINED + " AS SELECT * FROM " + MS +" LEFT JOIN " + MSMETA + " ON (" + MS + "." + MS+ID + "=" + MSMETA + "." + MSMETA+ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + MS + ID + "_idx ON " + MSJOINED + "(" + MS + ID + ")")
            self.execute( "CREATE UNIQUE INDEX " + MS + "_key ON " + MSJOINED + "(" + LDSBP + ID + "," + SBINDEX +")")
            self.execute( "GRANT SELECT ON " + MSJOINED + " TO public")
        
        connection.commit()
        self.cursor.close()
        connection.close()