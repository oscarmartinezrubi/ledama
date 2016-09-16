################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import config as lconfig
from ledama import tasksdistributor as td
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.MSP import MSP as MSPClass
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
from ledama.datamanagement.modules.archive.fdt.GetFDTFileList import GetFDTFileList
from ledama.leddb.modules.edit.RemoveFromLEDDB import RemoveFromLEDDB
from ledama.nodes.NodeMonitorDaemon import *
 
class UpdateLEDDBTarget(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('store','s','Store',helpmessage=' where the data is.',choice=utils.REMOTE_STORES)
        options.add('numprocessors','p','Simultaneous processes',default=20)
        options.add('upstats','u','Update only the Target stats',default=False, helpmessage=' (does not add MSPs)')
        options.add('cleanldsbp','c','Only cleans LDSBPs',default=False)
        options.add('logspath', 'l', 'Logs path', default=lconfig.TARGET_FDT_UPDATE_FOLDER)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)               
        # the information
        information = """Update the LEDDB with the data in Target"""
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        directory = what
        connection = Connector(self.dbname,self.dbuser,self.dbhost).getConnection()
        updater = MSPUpdater(connection)
        diagUpdater = DiagnosticUpdater(connection)
        print identifier + ('_C%03d' % childIndex) + ('_G%03d' % grandChildIndex) + ('_T%03d' % taskIndex) + ' reading ' + directory 
        try:      
            (sizes, paths) = GetFDTFileList().getFileList(self.store, directory)
        except Exception,e:
            connection.close()
            raise Exception(directory) 
        for i in range(len(sizes)):
            absPath = paths[i]
            size = sizes[i]
            try:
                msp = MSPClass(absPath, self.store, size)
                msp.loadReferencingDataAkin(connection)
                updater.process(msp)
                diagUpdater.updateMeta(msp.msId, True, False, False)
            except Exception,e:   
                print str(e)
                print 'Error in: ' + absPath 
        connection.close()

    def getLogFileName(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        return what + '.log'

    def process(self, store,numprocessors,upstats,cleanldsbp,logspath,dbname,dbuser,dbhost):
        logsPath = None
        if logspath != '':
            logsPath = utils.formatPath(os.path.abspath(logspath))
            
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        # Get current time stamp, all the updated measurement sets will have a 
        # higher time stamp
        currentTimeStamp = utils.getCurrentTime() 
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        self.store = store
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        retValuesKo = []
        (dirList,availableSpace) = GetFDTFileList().getDirList(self.store)
        removedCounter = 0
        if not upstats and not cleanldsbp:
            children = []
            for i in range(len(dirList)):
                children.append(store)
            print 'List of directories: ' + ', '.join(dirList)
            
            retValuesKo = td.distribute(children, dirList, self.function, numprocessors, 1, logFolder = logsPath, getLogFileName = self.getLogFileName)[1]
            if len(retValuesKo) == 0:
                # If we detect any error we do not remove the old references
                # it may be an error in the Target-EoR line, so this info is not 
                # trustable
                print 'Cleaning old references'
                rows = LEDDBOps.select(connection, MSP, {(LASTCHECK):(currentTimeStamp,'<'),(HOST):store}, columnNames = [MSP+ID,PARENTPATH,NAME])
                for row in rows:
                    msppath = row[1] + '/' + row[2]
                    removedCounter += 1
                    print 'Removing ' + MSP + ' row ' + store + ': ' + msppath
                    LEDDBOps.delete(connection, MSP, {MSP+ID:row[0],})
                connection.commit()
                
        if not upstats and ((len(retValuesKo) == 0) or cleanldsbp):
            RemoveFromLEDDB().cleanStore(connection, store)

        usedsize = float(LEDDBOps.select(connection, MSP, {(HOST):store,}, columnNames = ['sum(' + MSP+'.'+SIZE + ')'])[0][0]/(1024.*1024.))
        try:
            avsize = float(availableSpace.replace('T',''))
            percentage = int(usedsize * 100 / (usedsize+avsize))
            connection.close()
            
            linesToWrite = utils.getCurrentTimeStamp() + '\n'
            linesToWrite += 'eor' + ' ' + str(percentage) + ' ' + str(availableSpace) + '\n'
            
            if lconfig.NODE_MONITOR_TYPE == 'db':
                connection = Connector(lconfig.NODE_MONITOR_DB_NAME, dbuser, lconfig.NODE_MONITOR_DB_HOST).getConnection()
                LEDDBOps.updateUniqueRow(connection, HOST, {NAME:store,}, dataForUpdating = {STORAGEMON:linesToWrite,},)
                connection.commit()
                connection.close()
            else:
                nodeFile = open(getStorageFilePath(store), "w")
                nodeFile.write(linesToWrite)
                nodeFile.close()
        except:
            print 'ERROR getting sizes in Target!'
        
        if removedCounter:
            print 'A total of ' + str(removedCounter) + ' MSPs were removed'
        
        if len(retValuesKo):
            errors = []
            for retValueKo in retValuesKo:
                errors.append(str(retValueKo))
            print 'The following directories could not be read (they are empty):' + ', '.join(errors)
        
