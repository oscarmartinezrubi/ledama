################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater

# This code must run in a node with the GRID software installed, before running this code you should run InitGRID module
class AddUberFTP(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputpath','i','Input path',helpmessage='of the LDS in specified host.')
        options.add('host','s','Host', default='lotar1.staging.lofar',helpmessage='containing the data')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('store','t','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        # the information
        information = 'Add the references to LEDDB from a path (the parent path) in specified host (must be accessible through uberftp)'
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self,inputpath,host,lds,versionindex,store,dbname,dbuser,dbhost):
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        updater = MSPUpdater(connection)      
        diagUpdater = DiagnosticUpdater(connection)
        inputpath = utils.formatPath(inputpath)
        for line in os.popen('uberftp ' + host + ' "ls ' + inputpath + '"').read().split('\n'):
            fields = line.split()
            if len(fields) == 9:
                try:
                    absPath = inputpath + '/' + fields[8]
                    msp = MSPClass(absPath, store, (int(fields[4]) / 1048576))
                    msp.loadReferencingDataAkin(connection)
                    if lds != '':
                        msp.lds = lds
                    if versionindex != '':
                        msp.versionIndex = int(versionindex)
                    updater.process(msp)
                    diagUpdater.updateMeta(msp.msId, True, False, False)
                except Exception,e:   
                    print str(e)
                    print 'Error in:'
                    print absPath    
        connection.close()