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
from ledama.leddb.Naming import *
from ledama.leddb.MSPUpdater import MSPUpdater
from ledama.leddb.DiagnosticUpdater import DiagnosticUpdater
from ledama.leddb import LEDDBOps
 
class AddEmptyLDSBP(LModule):
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('inputdplist','i','Input DataProducts list',helpmessage=', use https://lofar.astron.nl/mom3/. Find your observation and beam. Click in Details and DataProducts and copy/paste the table in a file.')
        options.add('versionindex','v','Version Index', default = 0)
        options.add('inttime','t','Integration time')
        options.add('bandwidth','b','Bandwidth of each SB in MHz')
        options.add('numchan','c','Number of channels')
        options.add('numprocessors', 'p', 'Simultaneous processes', default = 20)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)     
        # the information
        information = """Add a LDSBP and the MSs contained in the inputhtml. This is only used to add frequency information of the SBs of a LDS/version. Note that none MSP is added!"""
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, inputdplist,versionindex,inttime,bandwidth,numchan,numprocessors,dbname,dbuser,dbhost):
        if self.userName not in lconfig.FULL_ACCESS_USERS:
            print 'Only ' + ','.join(lconfig.FULL_ACCESS_USERS) + ' can execute this code'
            return
        connection = Connector(dbname,dbuser,dbhost).getConnection()
        diagUpdater = DiagnosticUpdater(connection)
        for line in open(inputdplist,'r').read().split('\n'):
            fields = line.split()
            if len(fields) == 12:
                msp = MSPClass(fields[0], utils.EOR, 0)
                msp.loadReferencingDataAkin(connection)
                if msp.ldsbId == None:
                    connection.close()
                    print 'ERROR: Can not add LDSBP that have not related already added LDSB'
                    return
                else:
                    print 'Adding ' + msp.name
                    msp.versionIndex = versionindex
                    msp.intTime = float(inttime)
                    msp.centralFrequency =float(fields[10])
                    msp.totalBandwidth = bandwidth
                    msp.numChan = numchan
                    msp.ldsbpDescr =  str(msp.numChan) + ' ' + str(int(round(msp.intTime))) 
                    #Get the ID of the related LSDP
                    dataForLDSBPSelection = {}
                    dataForLDSBPSelection[LDSB+ID] = msp.ldsbId
                    dataForLDSBPSelection[STORE] = msp.store
                    dataForLDSBPSelection[INTTIME] = msp.intTime
                    dataForLDSBPSelection[NUMCHAN] = msp.numChan
                    dataForLDSBPSelection[VERSION] = msp.versionIndex
                    dataForLDSBPSelection[RAW] = msp.raw
                    dataForLDSBPSelection[TAR] = msp.tar
                    dataForLDSBPSelection[BVF] = msp.bvf
                    
                    dataForLDSBPUpdating = {}
                    dataForLDSBPUpdating[ADDDATE] = utils.getCurrentUTCTime()
                    dataForLDSBPUpdating[DESCR] = msp.ldsbpDescr
                    dataForLDSBPUpdating[FLAGGED] = msp.flagged
                    dataForLDSBPUpdating[AVERAGED] = msp.averaged
                    dataForLDSBPUpdating[CALIBRATED] = msp.calibrated
                    dataForLDSBPUpdating[DIRDEPCAL] = msp.ddCal
                    dataForLDSBPUpdating[DIRINDEPCAL] = msp.diCal
                    
                    lofarDataSetBeamProductId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(connection, LDSBP, dataForLDSBPSelection, dataForLDSBPUpdating, [LDSBP+ID,], updateOnlyIfRowMissing = True, updateOnlyIfColumnMissing = True)) 
            
                    # Get the ID of the related MS
                    dataForMSSelection = {}
                    dataForMSSelection[LDSBP + ID] = lofarDataSetBeamProductId
                    dataForMSSelection[SBINDEX] = msp.sbIndex
                    dataForMSUpdating = {}
                    dataForMSUpdating[CENTFREQ] = msp.centralFrequency
                    dataForMSUpdating[BW] = msp.totalBandwidth
                            
                    # We try to get the MSI Id from the data
                    msId = LEDDBOps.getColValue(LEDDBOps.updateUniqueRow(connection, MS, dataForMSSelection, dataForMSUpdating, [MS+ID,], updateOnlyIfColumnMissing = True, toPrint = self.debug))
                    diagUpdater.updateMeta(msId, False, False, False)
        connection.close()