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
 
class AddCatFileList(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('catfile','i','Input CatFileList')
        options.add('hostpath','s','Host path', default='srm://srm.grid.sara.nl:8443',helpmessage=', specify the host:port part of the path (including srm://). You can get this information from the beginning of each path.')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('store','t','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)     
        # the information
        information = """Add references to LEDDB from a CatFileList  (file with ms per line with 6 fields, the third field must be the path and the forth the size) and host:port. No connection is required here."""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, catfile,hostpath,lds,versionindex,store,dbname,dbuser,dbhost):
        
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.hostpath = hostpath
        self.lds = lds
        self.version = versionindex
        if store not in utils.REMOTE_STORES:
            print 'You must provide a store of  ' + ','.join(utils.REMOTE_STORES)
            return
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        updater = MSPUpdater(connection)
        diagUpdater = DiagnosticUpdater(connection)
        tfile = (open(catfile, "r")).read()
        lines = tfile.split('\n')
        for line in lines:
            if not line.starts_with('#'):
                fields = line.split('\t')
                if len(fields) == 4:
                    (dataproduct_id, absPath, size, md5_hash) = fields
                    aux = ''
                    try:
                        if absPath.count(hostpath):
                            msp = MSPClass(absPath.replace(hostpath,''), store, size)
                            msp.loadReferencingData()
                            if lds != '':
                                msp.lds = lds
                            if self.version != '':
                                msp.versionIndex = int(self.version)
                            updater.process(msp)
                            diagUpdater.updateMeta(msp.msId, True, False, False)
                        else:
                            aux += absPath + ' not found\n'
                    except Exception,e:   
                        aux += str(e) + '\n'
                        aux += 'Error in:' + '\n'
                        aux +=  absPath + '\n'
                    if aux != '':
                        print aux
        connection.close()