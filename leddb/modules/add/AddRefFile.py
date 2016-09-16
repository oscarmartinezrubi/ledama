################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
from ledama.ReferenceFile import ReferenceFile

# Return a string with the concatenation of the md5sum of the contained files. 
# This method is recursive
def addMS(absPath, diagnosticStr,lds,versionStr,dbname, dbuser, dbhost):
    connection = Connector(dbname, dbuser, dbhost).getConnection()
    updater = MSPUpdater(connection)
    diagUpdater = DiagnosticUpdater(connection)
    try:
        msp = MSPClass(absPath, utils.getHostName(), msoperations.getSize(absPath))
        msp.loadReferencingData()
        if lds != '':
            msp.lds = lds
        if versionStr != '':
            msp.versionIndex = int(versionStr)
        updater.process(msp)
        if diagnosticStr == 'True':
            hasGain = diagUpdater.updateGain(msp)[0]
            hasQuality = diagUpdater.updateQuality(msp)[0]
            # Update the Meta information of this ms
            diagUpdater.updateMeta(msp.msId, True, hasGain, hasQuality)
        aux = ''
    except Exception,e:   
        aux = 'Error in:' + '\n' + str(e)
    connection.close()
    return aux

class AddRefFile(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('reffile','i','Input RefFile')
        options.add('lds','l','LDS name', mandatory=False, helpmessage='to be used. By default the name extracted from the path will be used')
        options.add('versionindex','v','Version Index', mandatory=False, helpmessage=' to be used. By default the version extracted from the path will be used')
        options.add('diagnostic', 'd', 'Include diagnostic updated?', default = False)
        options.add('numprocessors', 'p', 'Simultaneous processes', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 20)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)     
        options.add('initfile', 's', 'Init file', helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        # the information
        information = 'Add the references to LEDDB given a RefFile'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Function used for the tasksdistributor
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (absPath,size) = what
        currentNode = utils.getHostName()
        # Define command        
        if node.count('node'):
            remoteCommand = "source " + self.initfile + ' ; python -c "import ' + __name__ + '; print ' + __name__ + '.' + addMS.__name__ + '(\\\"' + absPath +'\\\",\\\"' + str(self.diagnostic) +'\\\",\\\"' + str(self.lds) +'\\\",\\\"' + str(self.version) +'\\\",\\\"' + self.dbname +'\\\",\\\"' + self.dbuser +'\\\",\\\"' + self.dbhost +'\\\")"'
            if node != currentNode:
                remoteCommand =  'ssh ' + node + " '" + remoteCommand + "'"
            (out, err) = td.execute(remoteCommand)     
            if err != '':
                raise Exception(err)
            for mes in out.split('\n'):
                if mes != '':
                    print mes    
        else:
            connection = Connector(self.dbname, self.dbuser , self.dbhost).getConnection()
            updater = MSPUpdater(connection) 
            diagUpdater = DiagnosticUpdater(connection)
            msp = MSPClass(absPath, node, size)
            msp.loadReferencingDataAkin(connection)
            updater.process(msp)
            if self.diagnostic:
                diagUpdater.updateMeta(msp.msId, True, False, False)
            connection.close()

    def process(self, reffile,lds,versionindex,diagnostic,numprocessors,numnodes,dbname,dbuser,dbhost,initfile):
        self.initfile = os.path.abspath(initfile)
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.diagnostic = diagnostic
        self.lds = lds
        self.version = versionindex
        referenceFile = ReferenceFile(reffile)
        whats = []
        for i in range(len(referenceFile.absPaths)):
            whats.append((referenceFile.absPaths[i], referenceFile.sizes[i]))
        retValuesKo = td.distribute(referenceFile.nodes, whats, self.function, numprocessors, numnodes, dynamicLog = False)[1]
        td.showKoAll(retValuesKo)