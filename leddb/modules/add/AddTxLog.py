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
 
class AddTxLog(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputtx','i','Input TransferLog path')
        options.add('hostpath','s','Host path', default='srm://srm.grid.sara.nl:8443',helpmessage=', specify the host:port part of the path (including srm://). You can get this information from the beginning of each path.')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('store','t','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('numprocessors', 'p', 'Simultaneous processes', default = 20)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        # the information
        information = """Add references to LEDDB from a TransferLog  (file with ms per line with 6 fields, the third field must be the path and the forth the size) and host:port. No connection is required here."""
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self,inputtx,hostpath,lds,versionindex,store,numprocessors,dbname,dbuser,dbhost):
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        if store not in utils.REMOTE_STORES:
            print 'You must provide a store of  ' + ','.join(utils.REMOTE_STORES)
            return
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        updater = MSPUpdater(connection)
        diagUpdater = DiagnosticUpdater(connection)
        tfile = (open(inputtx, "r")).read()
        lines = tfile.split('\n')
        for line in lines:
            fields = line.split(' ')
            if len(fields) == 6:
                absPath = fields[2]
                size = int(fields[3]) / 1048576
                try:
                    if absPath.count(hostpath):
                        msp = MSPClass(absPath.replace(hostpath,''), store, size)
                        msp.loadReferencingData()
                        if lds != '':
                            msp.lds = lds
                        if versionindex != '':
                            msp.versionIndex = int(versionindex)
                        updater.process(msp)
                        diagUpdater.updateMeta(msp.msId, True, False, False)
                    else:
                        print absPath + ' not found'
                except Exception,e:   
                    print str(e)
                    print 'Error in:'
                    print absPath
        connection.close()