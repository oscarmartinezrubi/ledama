################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import re
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater

class AddRemoteUnaccessiblePath(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputpath','i','Input path', helpmessage=' path of the LDS in specified host. Specify the complete path to one of the SBs')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('store','s','Store', helpmessage=' where data is.',choice=utils.REMOTE_STORES)
        options.add('subbands','b','SubBands', helpmessage=' to add.', default='000-243')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)   
        # the information
        information = 'Add the references to LEDDB from a path in specified host (this is for unaccessible hosts)'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self,inputpath,lds,versionindex,store,subbands,dbname,dbuser,dbhost):
        
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        updater = MSPUpdater(connection)      
        diagUpdater = DiagnosticUpdater(connection)
        sbInSample = (re.search('SB[0-9]*', inputpath)).group(0)
        for band in utils.getSubBandsToUse(subbands):
            aux = ''
            try:
                absPath = inputpath.replace(sbInSample, band)
                msp = MSPClass(absPath, store, -1)
                msp.loadReferencingDataAkin(connection)()
                if lds != '':
                    msp.lds = lds
                if versionindex != '':
                    msp.versionIndex = int(versionindex)
                updater.process(msp)
                diagUpdater.updateMeta(msp.msId, True, False, False)
            except Exception,e:   
                aux += str(e) + '\n'
                aux += 'Error in:' + '\n'
                aux +=  absPath + '\n'
            if aux != '':
                print aux
        connection.close()