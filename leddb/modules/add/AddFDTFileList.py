################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
 
class AddFDTFileList(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('fdtfile','i','Input FDTFileList')
        options.add('store','s','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('numprocessors', 'p', 'Simultaneous processes', default = 20)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)     
        # the information
        information = """Add references to LEDDB from a FDTFileList  (file with ms per line with 2 fields, first field is size and second is the relative path. This path is relative to the eor root"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, fdtfile,store,lds,versionindex,numprocessors,dbname,dbuser,dbhost):
        
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.store = store
        self.lds = lds
        self.version = versionindex
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        updater = MSPUpdater(connection)
        diagUpdater = DiagnosticUpdater(connection)
        tfile = (open(fdtfile, "r")).read()
        lines = tfile.split('\n')
        for line in lines:
            fields = line.split()
            if len(fields) == 2:
                aux = ''
                absPath = fields[1]
                size = int(fields[0])
                try:
                    msp = MSPClass(absPath, self.store, size)
                    msp.loadReferencingDataAkin(connection)
                    if lds != '':
                        msp.lds = lds
                    if self.version != '':
                        msp.versionIndex = int(self.version)
                    updater.process(msp)
                    diagUpdater.updateMeta(msp.msId, True, False, False)
                except Exception,e:   
                    aux += str(e) + '\n'
                    aux += 'Error in:' + '\n'
                    aux +=  absPath + '\n'
                if aux != '':
                    print aux
        connection.close()
