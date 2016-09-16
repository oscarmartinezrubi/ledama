################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Connector import *
from ledama.leddb.Naming import *
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama import utils
from ledama.leddb import LEDDBOps
from ledama import config as lconfig
from ledama.leddb.LDSBPUpFile import LDSBPUpFile

LDSBP_EDIT_COLS = [DESCR, MAINUSER, FLAGGED, AVERAGED, CALIBRATED, DIRDEPCAL, DIRINDEPCAL] 
    
class UpdateLDSBP(LModule):
    def __init__(self, userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('lids','i','LDSBP Ids', helpmessage=' to be updated from the contents of the given file')
        options.add('lfile','f','LDSBPUpFile', helpmessage=' which contains the info to be updated')
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)      
        # the information
        information = """update the LEDDB information from the provided LDSBPUpFile"""
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def update(self, connection, lid, ldsbupFile):
        newValues = [ldsbupFile.description, ldsbupFile.mainUser, ldsbupFile.flagged, ldsbupFile.averaged, ldsbupFile.calibrated, ldsbupFile.dirDepCalibrated , ldsbupFile.dirIndepCalibrated]
        dataToUpdate = {}
        for i in range(len(LDSBP_EDIT_COLS)):
            if newValues[i] != None:    
                dataToUpdate[LDSBP_EDIT_COLS[i]] = newValues[i]
        try:
            if len(dataToUpdate):
                LEDDBOps.update(connection, LDSBP, dataToUpdate, {LDSBP+ID:lid,})
            else:
                print "None changes have been done!"
                return
        except Exception,e:
            print 'ERRROR updating LDSBP ' + str(lid) + ':'
            print str(e)
        # Remove old appFiles and appRuns
        approws = LEDDBOps.select(connection, [APPRUN,], {APPRUN+'.'+LDSBP+ID:lid}, [APPRUN+'.'+APPRUN+ID,])
        
        if len(approws):
            for (appRunId,) in approws:
                LEDDBOps.delete(connection, APPFILE, {APPFILE+'.'+APPRUN+ID:appRunId,})
        LEDDBOps.delete(connection, APPRUN, {APPRUN+'.'+LDSBP+ID:lid})
        
        for i in range(len(ldsbupFile.appOrders)):
            if not ldsbupFile.appCommented[i]:
                appRunId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(connection, APPRUN, {LDSBP+ID:lid,ORDERINDEX:ldsbupFile.appOrders[i],APPNAME:ldsbupFile.appNames[i],}, {DESCR:ldsbupFile.appDescriptions[i]}, [APPRUN+ID,]))         
                appfiles = ldsbupFile.appFiles[i].split(',')
                for appfile in appfiles:
                    if appfile != '':
                        LEDDBOps.updateUniqueRow(connection, APPFILE, {APPRUN+ID:appRunId, FILEPATH:appfile.strip()})
        
    def process(self, lids, lfile, dbname,dbuser,dbhost):
        if not (self.userName in lconfig.FULL_ACCESS_USERS or dbname != DEF_DBNAME):
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        connection = Connector(dbname, dbuser, dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return
        ldsbupFile = LDSBPUpFile(lfile)
        ldsbupFile.read()
        for lid in utils.getElements(lids):
            self.update(connection, lid, ldsbupFile)            
            print 'Update LDSBP ' + str(lid)
        connection.close()
            