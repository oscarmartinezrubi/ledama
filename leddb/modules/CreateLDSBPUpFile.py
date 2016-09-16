################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import config as lconfig
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.query.QueryManager import QueryManager
from ledama.leddb.LDSBPUpFile import LDSBPUpFile
from pyrap import tables as pt
from ledama.leddb import LEDDBOps
    
class CreateLDSBPUpFile(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('ldsbpid','i','LDSBPId')
        options.add('output','o','Output LDSBPUpFile',helpmessage=', this file is created from current information on the LEDDB and the HISTORY tables. Please modify this file and commit the changes using LModule UpdateLDSBP. If APP info is already available in LEDDB it will also be output in the file but will be commented')
        options.add('leddb','l','Use only LEDDB',default=False, helpmessage=', this creates the file only from information in the LEDDB')
        options.add('allhistory','a','Use all history',default=False, helpmessage=', if activated, this will use all the history table information. By default only the entries after 1 day before LEDDB addition are shown')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)        
        # the information
        information = """
        Create a LDSBPUpFile from HISTORY tables and current info in the LEDDB. 
        If you already updated the information and just want to see the current content you want to use option -l or --leddb
        Regarding the HISTORY tables, we assume all the MSs of the LDSBP have the same HISTORY table.
        """
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
    
    def process(self, ldsbpid, output, leddb, allhistory, dbname,dbuser,dbhost):
        ignore = leddb
        if os.path.isfile(output):
            print 'Error: ' + output + ' already exists'
            return 
        
        # Crete the conenction and the cursor
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        cursor = connection.cursor()
        
        ldsbpid = int(ldsbpid)
        ldsbprows = LEDDBOps.select(connection, [LDSBP,], {LDSBP+'.'+LDSBP+ID:ldsbpid}, 
            [LDSBP+'.'+DESCR+','+LDSBP+'.'+MAINUSER+','+LDSBP+'.'+ADDDATE+','+LDSBP+'.'+FLAGGED+','+LDSBP+'.'+AVERAGED+','+LDSBP+'.'+CALIBRATED+','+LDSBP+'.'+DIRDEPCAL+','+LDSBP+'.'+DIRINDEPCAL])
        
        # Get the current info from the LEDDB for this LDSBP
        if len(ldsbprows):
            (description, mainUser, addDate, flagged, averaged, calibrated,dirDepCalibrated, dirIndepCalibrated) = ldsbprows[0]
        else:
            print 'LDSBP was not found'
            return
        
        # The fields required for the LDSBPUpFile
        appOrders = []
        appNames = []
        appDescriptions = []
        appFiles = []
        appCommented = []
        
        if not ignore:
        
            # Get from the LEDDB the host and path to one of the MS of the LDSBP
            qm = QueryManager()
            queryOption = MSP_KEY
            names = [HOST, PARENTPATH, NAME]
            qc = qm.initConditions()
            qm.addCondition(queryOption, qc, LDSBP+ID, ldsbpid, '=')          
            (query, queryDict) = qm.getQuery(queryOption, qc, names, [SBINDEX,], limit = 1)
            qm.executeQuery(connection, cursor, query, queryDict)
            mspFound = True
            if cursor.rowcount:
                (host,parentPath,msName) = cursor.fetchone()
            else:
                mspFound = False
                print 'None MSP found for the given LDSBP'
            
            if mspFound and host.count('node'):    
                absPath = '/net/' + host + parentPath + '/' + msName
                if os.path.islink(absPath):
                    absPath = '/net/' + host + os.readlink(absPath)
                
                # get the HISTORY table path
                historyPath = absPath + '/' + 'HISTORY'
                timeStamp = None
                if addDate != None and not allhistory:
                    timeStamp = utils.getTimeStamp(addDate - (86400))
                
                try:
                    historyDates = (pt.taql('calc ctod(mjdtodate([select TIME from ' + historyPath + ']))'))['0']
                except:
                    historyDates = (pt.taql('calc ctod(mjdtodate([select TIME from ' + historyPath + ']))'))['array']
            
                # Also for printing information
                appParams = []
                appDates = []
                
                thistory = pt.table(historyPath, readonly=True, ack=False)
                for historyIndex in range(len(historyDates)):
                    appOrders.append(historyIndex)
                    appNames.append(thistory.getcell('APPLICATION',historyIndex))
                    appDescriptions.append('')
                    appFiles.append('')
                    appParams.append(thistory.getcell('APP_PARAMS',historyIndex))
                    appDates.append(historyDates[historyIndex])
                    if timeStamp == None or historyDates[historyIndex] > timeStamp:
                        appCommented.append(False)
                    else:
                        appCommented.append(True)
        
                if len(appOrders):
                    if timeStamp != None:
                        print '#INFORMATION FROM HISTORY TABLE (APPs RUN BEFORE ' + str(timeStamp) + ' ARE COMMENTED)'
                    else:
                        print '#INFORMATION FROM HISTORY TABLE (ALL APPs ARE SHOWN)'
                    print '\t'.join(('#ORDER', '      DATE         ', 'APP_NAME', 'APP_PARAMS'))
                for i in range(len(appOrders)):
                    appName = appNames[i]
                    appIndex = appOrders[i]
                    extraParam = ''
                    if len(appParams) > 20:
                        extraParam = ' ...'
                    if appCommented[i]:
                        indexString = ('#  %03d' % appIndex)
                    else:
                        indexString = ('   %03d' % appIndex)
                    print '\t'.join([indexString, utils.removeDecimal(appDates[i]), appName, ('|'.join(appParams[i][0:20])) + extraParam])
            
            # Get the app info from LEDDB
        approws = LEDDBOps.select(connection, [APPRUN,], {APPRUN+'.'+LDSBP+ID:ldsbpid}, [APPRUN+'.'+APPRUN+ID,APPRUN+'.'+ORDERINDEX,APPRUN+'.'+APPNAME,APPRUN+'.'+DESCR,], orderBy = APPRUN+'.'+ORDERINDEX) 
        
        if len(approws):
            if not ignore:
                print 'This LDSBP already had APP info in the LEDDB, it will also be written but commented'
            for (appRunId, orderIndex, appName, appDescription) in approws:
                appOrders.append(orderIndex)
                appNames.append(appName)
                if appDescription == None:
                    appDescription = ''
                appDescriptions.append(appDescription)
                appCommented.append(not ignore)
                appfilesrows = LEDDBOps.select(connection, [APPFILE,], {APPFILE+'.'+APPRUN+ID:appRunId}, [APPFILE+'.'+FILEPATH,])
                appFilesString = ''
                if len(appfilesrows):
                    for (filePath,) in appfilesrows:
                        appFilesString += filePath + ','
                    appFilesString = appFilesString[:-1]
                appFiles.append(appFilesString)     
                
        # Close both connection and cursor objects
        cursor.close()
        connection.close()
        
        ldsbupFile = LDSBPUpFile(output, description, mainUser, flagged, averaged, calibrated,
                 dirDepCalibrated, dirIndepCalibrated, appOrders, appNames, appDescriptions, appFiles, appCommented)
        ldsbupFile.validate()
        ldsbupFile.write()
        
        print 'LDSBPUp file has been written in ' + output
        if not ignore:
            print 'Modify it and use the LModule UpdateLDSBP to commit your changes'
        
