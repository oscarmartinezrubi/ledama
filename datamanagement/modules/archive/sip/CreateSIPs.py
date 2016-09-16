################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, numpy
from datetime import datetime
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.ReferenceFile import ReferenceFile
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST, Connector
from ledama.leddb.Naming import *
from ledama.leddb import LEDDBOps
from ledama.datamanagement.sip import sipapi
from ledama.datamanagement.sip.SIPIOHandler import SIPIOHandler
from ledama.leddb.LDSBPUpFile import LDSBPUpFile
from ledama.leddb.modules.edit.UpdateLDSBP import UpdateLDSBP
    
class CreateSIPs(LModule):    
    def __init__(self,userName = None):
        self.userName = userName
        if self.userName == None:
            self.userName = utils.getUserName()
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('isips','s','Input SIPs folder')
        options.add('osips','o','Output SIPs folder')
        options.add('lfile','f','LDSBPUpFile', mandatory=False, helpmessage=' which contains the information related to the processing done to the data. If this is not provided we try to extract the information from the LEDDB (in this case the user must have previously updated the information to LEDDB with UpdateLDSBP module)')
        options.add('numprocessors', 'p', 'Number of processes', default = 16)
        options.add('dbname','w','DB name',default=DEF_DBNAME)
        options.add('dbuser','y','DB user',default=self.userName)
        options.add('dbhost','z','DB host',default=DEF_DBHOST)
        
        # the information
        information = """Create SIPs for all the measurements sets pointed by the refFile. 
We assume all the MSs are within the same LDBSBP. For the new SIPs the LDSBP information is used (and the LDSBPUpFile if specified).
For each MS its new SIP is a direct update from the input SIP (and they are related trough the SB index)"""
        # Initialize the parent class
        LModule.__init__(self, options, information)
        
    def functionGetDP(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (i, absPath, node, size, sipPath) = what
        sipioh = SIPIOHandler(sipPath)
        sip = sipioh.getSIP()
        dp = sip.get_dataProduct()
        integrationTime = None
        channelWidth = None
        if i == 0:
            integrationTime = float(dp.get_integrationInterval().get_valueOf_())
            channelWidth = float(dp.get_channelWidth().get_valueOf_()) # Usually in KHz
        return (i, integrationTime, channelWidth, dp)
    
    def functionCreateSIP(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (i, absPath, node, size, sipPath) = what
        parentPath = msoperations.getParentPath(absPath)
        msName = msoperations.getMeasurementSetName(absPath)
        connection = Connector(self.dbname, self.dbuser, self.dbhost).getConnection()
        rows = LEDDBOps.select(connection, [MS,MSP,LDSBP], 
                               dataForSelection = {MSP+'.'+MS+ID:MS+'.'+MS+ID,MS+'.'+LDSBP+ID:LDSBP+'.'+LDSBP+ID, 
                                                   NAME:msName, PARENTPATH:parentPath, HOST:node}, 
                               columnNames = [MS+'.'+MS+ID,LDSBP+'.'+INTTIME,LDSBP+'.'+NUMCHAN,MS+'.'+CENTFREQ,MS+'.'+BW])
        connection.close()
        if not len(rows):
            raise Exception ('LEDDB does not contain a entry for ' + node + ':' + absPath)
        (msId, intTime, numChan, centFreq, bandWidth) = rows[0]
        if centFreq == None or intTime == None or intTime < 0:
            raise Exception('Freq. and/or time information are not present for ' + node + ':' + absPath + '. Maybe you have to manually fill the properties (use LEDDBOps.copyProp)') 
                   
        sipioh = SIPIOHandler(sipPath)
        sip = sipioh.getSIP()
        cdp = sip.get_dataProduct()
        centralFrequency = sipapi.Frequency()
        centralFrequency.set_valueOf_(centFreq)
        centralFrequency.set_units('MHz')
        cdp.set_centralFrequency(centralFrequency)
        cdp.set_channelsPerSubband(numChan)
        channelWidth = sipapi.Frequency()
        channelWidth.set_valueOf_(bandWidth * 1000. / numChan)
        channelWidth.set_units('kHz')
        cdp.set_channelWidth(channelWidth)
        dataProductIdentifier= sipapi.IdentifierType()
        dataProductIdentifier.set_source('EoR')
        dataProductIdentifier.set_identifier(msId)
        dataProductIdentifier.set_name(self.ldsName + '_' + ('%03d'%self.versionIndex) + '/' + msName)
        cdp.set_dataProductIdentifier(dataProductIdentifier)
        cdp.set_fileName(msName)
        integrationInterval = sipapi.Time()
        integrationInterval.set_valueOf_(intTime)
        integrationInterval.set_units('s')
        cdp.set_integrationInterval(integrationInterval)
        cdp.set_processIdentifier(self.processIdentifier)
        cdp.set_size(size)
        sip.set_dataProduct(cdp)
        
        sip.add_pipelineRun(self.pipelineRun)
        if self.parset != None:
            sip.add_parset(self.parset)
            
        for newRDP in self.inputDataProducts:
            sip.add_relatedDataProduct(newRDP)
        
        outputSip = self.osips + '/' + sipioh.getSIPPath(msoperations.getMeasurementSetName(absPath))
        sipioh.write(outputSip)
        
        return (i, outputSip)
    
    def process(self,reffile,isips,osips,lfile,numprocessors,dbname,dbuser,dbhost):
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbhost = dbhost
        currentNode = utils.getHostName()
        # Check the input SIPs folder 
        isips = os.path.abspath(isips)
        if not os.path.isdir(isips):
            print isips + ' does not exist '
            return
        sipSBDict = {} # Fill dictionary with key: SB index, value: SIP path
        for sipfile in os.listdir(isips):
            sipSBDict[msoperations.getSBIndex(sipfile)] = isips + '/' + sipfile
        
        # Check and open the input reffile
        try:
            referenceFile = ReferenceFile(reffile)
        except Exception,e:
            print str(e)
            return
        
        # We take the first SB as a reference
        refAbsPath = referenceFile.absPaths[0]
        refParentPath = msoperations.getParentPath(refAbsPath)
        refMsName = msoperations.getMeasurementSetName(refAbsPath)
        self.ldsName = msoperations.getLDSName(refAbsPath)
        self.versionIndex = msoperations.getVersionIndex(refAbsPath)
        refNode = referenceFile.nodes[0]
        
        whats = []
        identifiers = []
        self.inputDataProducts = []
        for i in range(len(referenceFile.absPaths)):
            absPath = referenceFile.absPaths[i]
            sbIndex = msoperations.getSBIndex(absPath)
            if (self.ldsName != msoperations.getLDSName(absPath)) or (self.versionIndex != msoperations.getVersionIndex(absPath)):
                print 'Error: all data must related to the same LDS and version'
                return
            if not sbIndex in sipSBDict:
                print 'Error: No SIP for ' + absPath
                return
            whats.append((i, absPath, referenceFile.nodes[i], referenceFile.sizes[i], sipSBDict[sbIndex]))
            identifiers.append(currentNode)
            self.inputDataProducts.append(None)
                
        # Create output SIPs folder
        self.osips = os.path.abspath(osips)
        os.system('mkdir -p ' + self.osips)
        
        connection = Connector(self.dbname, self.dbuser, self.dbhost).getConnection()
        if connection == None:
            print 'Error getting the connection'
            return  
        
        # Use first SB to get information about the LDSBP 
        # (i.e. description, mainUser, addDate, intTime, numChan, isCalibrated, centFreq, bandWidth)
        rows = LEDDBOps.select(connection, [MS,MSP,LDSBP], 
                               dataForSelection = {MSP+'.'+MS+ID:MS+'.'+MS+ID,MS+'.'+LDSBP+ID:LDSBP+'.'+LDSBP+ID, NAME:refMsName, PARENTPATH:refParentPath, HOST:refNode}, 
                               columnNames = [LDSBP+'.'+LDSBP+ID,LDSBP+'.'+DESCR,LDSBP+'.'+MAINUSER,LDSBP+'.'+ADDDATE,LDSBP+'.'+INTTIME,LDSBP+'.'+NUMCHAN,LDSBP+'.'+CALIBRATED,MS+'.'+CENTFREQ,MS+'.'+BW])
        if not len(rows):
            print 'LEDDB does not contain a entry for ' + refNode + ':' + refAbsPath
            connection.close()
            return
        
        (ldsbpid, description, mainUser, addDate, intTime, numChan, isCalibrated, centFreq, bandWidth) = rows[0]
        if None in (intTime,numChan,centFreq,bandWidth):
            print 'LDSBP ID ' + str(ldsbpid) + ' does not have enough information: intTime, numChan. Also at least the first MS must have centFreq and BW'
            connection.close()
            return
        
        # Get timing information. When the LDSBP was added to the LEDDB and also current time
        initialTimeStamp = utils.getTimeStamp(addDate)
        initialTime = addDate
        currentTimeStamp = utils.getCurrentUTCTimeStamp()
        currentTime = utils.convertTimeStamp(currentTimeStamp)
        
        # Get the app info for this ldsbp
        approws = LEDDBOps.select(connection, [APPRUN,], {APPRUN+'.'+LDSBP+ID:ldsbpid}, [APPRUN+'.'+APPRUN+ID,APPRUN+'.'+ORDERINDEX,APPRUN+'.'+APPNAME,APPRUN+'.'+DESCR,], orderBy = APPRUN+'.'+ORDERINDEX) 
        if not len(approws):
            print 'LEDDB does not contain app entry for LDSBPId ' + str(ldsbpid)
            print 'Use ExecuteLModule CreateLDSBPUpFile and ExecuteLModule UpdateLDSBP to add info in the LEDDB of the processing done do this data or provide a LDSBPUpFile'
            connection.close()
            return
        # Define stratedy name, description and parset contents
        appNames = []
        appDescriptions = []
        parsetContents = ''
        if lfile != '':
            ldsbupFile = LDSBPUpFile(lfile)
            ldsbupFile.read()
            print 'Updating LEDDB for LDSBP ID ' + str(ldsbpid)
            UpdateLDSBP().update(connection, ldsbpid, ldsbupFile)
            (description, mainUser, isCalibrated) = (ldsbupFile.description, ldsbupFile.mainUser, ldsbupFile.calibrated)
            for i in range(len(ldsbupFile.appOrders)):
                if not ldsbupFile.appCommented[i]:
                    (orderIndex,appName, appDescription) = (ldsbupFile.appOrders[i],ldsbupFile.appNames[i],ldsbupFile.appDescriptions[i].strip())
                    appNames.append(str(orderIndex) + '-' + appName)
                    if appDescription != None and appDescription != '':
                        appDescriptions.append(str(orderIndex) + '-' + appName + ':' + appDescription)
                    appfiles = ldsbupFile.appFiles[i].split(',')
                    for appfileindex in range(len(appfiles)):
                        filePath = appfiles[appfileindex].strip()
                        if os.path.isfile(filePath):
                            parsetContents += '\n-----CONTENTS OF ' + str(orderIndex) + '-' + appName +':'+str(appfileindex)+'-----\n'+open(filePath, "r").read()+'\n' 
                        else:
                            print 'No contents available for ' + filePath
                            connection.close()
                            return
        else:
            for (appRunId, orderIndex, appName, appDescription) in approws:
                if not isCalibrated and appName.lower().count('bbs'):
                    print 'LEDDB says data is not calibrated but it has been processed with BBS. Update info for LDSBP ' + str(ldsbpid)
                    connection.close()
                    return    
                appNames.append(str(orderIndex) + '-' + appName)
                if appDescription != None and appDescription != '':
                    appDescriptions.append(str(orderIndex) + '-' + appName + ':' + appDescription)
                    
                appfilesrows = LEDDBOps.select(connection, [APPFILE,], {APPFILE+'.'+APPRUN+ID:appRunId}, [APPFILE+'.'+APPFILE+ID,APPFILE+'.'+FILEPATH,])
                if len(appfilesrows):
                    for (fileId, filePath) in appfilesrows:
                        if os.path.isfile(filePath):
                            parsetContents += '\n-----CONTENTS OF ' + str(orderIndex) + '-' + appName +':'+str(fileId)+'-----\n'+open(filePath, "r").read()+'\n' 
                        else:
                            print 'No contents available for ' + filePath
                            connection.close()
                            return
        strategyName = ','.join(appNames)
        strategyDescription = description
        if len(appDescriptions):
            strategyDescription +=  '. ' + ', '.join(appDescriptions)
        connection.close()
        
        # We use task distributor to use many cores to get the dataProducts from the SIPs 
        oldIntegrationTime = None
        oldChannelWidth = None        
        (retValuesOk,retValuesKo) = td.distribute(identifiers, whats, self.functionGetDP, numprocessors, 1)
        if len(retValuesKo):
            print 'Some errors happened while extracting information from input SIPs: '
            for retValueKo in retValuesKo:
                print str(retValueKo)
        for retValueOk in retValuesOk:
            (index, integrationTime, channelWidth, dp) = retValueOk
            self.inputDataProducts[index] = dp
            if index == 0:
                oldIntegrationTime = integrationTime
                oldChannelWidth = channelWidth
        
        # We create the pipelineRun that will be added in all created SIPs
        # and fill all its fields
        if isCalibrated:
            self.pipelineRun = sipapi.CalibrationPipeline()
            self.pipelineRun.set_extensiontype_('sip:CalibrationPipeline')
            pipName = 'Calibration'
        else:
            self.pipelineRun = sipapi.AveragingPipeline()
            self.pipelineRun.set_extensiontype_('sip:AveragingPipeline')
            pipName = 'Averaging'
        
        pipelineRunName = 'LOFAR EoR ' + pipName + ' pipeline for ' + str(ldsbpid)
        if mainUser != None: 
            pipelineRunName += ' by ' + mainUser
        
        self.processIdentifier = sipapi.IdentifierType()
        self.processIdentifier.set_source('EoR')
        self.processIdentifier.set_name(pipelineRunName)
        self.processIdentifier.set_identifier(str(ldsbpid))
        self.pipelineRun.set_processIdentifier(self.processIdentifier)
        
        obsIdentifier = sipapi.IdentifierType()
        obsIdentifier.set_source('EoR')
        obsIdentifier.set_identifier(str(ldsbpid))
        self.pipelineRun.set_observationId(obsIdentifier)
        
        self.pipelineRun.set_strategyName(strategyName)
        self.pipelineRun.set_strategyDescription(strategyDescription)
        timefields = utils.changeTimeFormat(initialTimeStamp, finalTimeFormat = '%Y:%m:%d:%H:%M:%S').split(':')
        for i in range(len(timefields)):
            timefields[i] = int(timefields[i])
        startTime = datetime(*timefields)
        self.pipelineRun.set_startTime(startTime)
        self.pipelineRun.set_duration('PT' + str(int(currentTime - initialTime)) + 'S')
        self.pipelineRun.set_pipelineName(pipelineRunName)
        self.pipelineRun.set_pipelineVersion('n/a')
        
        sourceData = sipapi.DataSources()
        for i in range(len(referenceFile.absPaths)):
            dpi = self.inputDataProducts[i].get_dataProductIdentifier()
            sourceIdentifier = sipapi.IdentifierType()
            sourceIdentifier.set_source(dpi.get_source())
            sourceIdentifier.set_name(dpi.get_name())
            sourceIdentifier.set_identifier(dpi.get_identifier())
            sourceData.add_dataProductIdentifier(sourceIdentifier)
        self.pipelineRun.set_sourceData(sourceData)
        self.pipelineRun.set_timeIntegrationStep(int(numpy.round(oldIntegrationTime/intTime)))
        # bandwidth is in MHz, we convert it to KHz
        channelWidth = (bandWidth * 1000.) / numChan
        self.pipelineRun.set_frequencyIntegrationStep(int(numpy.round(channelWidth/oldChannelWidth)))
        self.pipelineRun.set_flagAutoCorrelations(False)
        self.pipelineRun.set_demixing(True)
        if isCalibrated:
            self.pipelineRun.set_skyModelDatabase('EoRSkyModel')
            self.pipelineRun.set_numberOfInstrumentModels(0)
        self.pipelineRun.set_numberOfCorrelatedDataProducts(len(referenceFile.absPaths))
        
        # Create parset object
        self.parset = None
        if parsetContents != '':
            self.parset = sipapi.Parset()
            parsetIdentifier = sipapi.IdentifierType()
            parsetIdentifier.set_source('EoR')
            parsetIdentifier.set_identifier(str(ldsbpid))
            self.parset.set_contents(parsetContents)
            self.parset.set_identifier(parsetIdentifier)
            self.pipelineRun.set_parset(parsetIdentifier)
        
        # We use distributor again to use many cores for the creation of the SIPs
        (retValuesOk,retValuesKo) = td.distribute(identifiers, whats, self.functionCreateSIP, numprocessors, 1)  
        if len(retValuesKo):
            print 'Some errors happened while generating the SIPs: '
            for retValueKo in retValuesKo:
                print str(retValueKo)
        if len(retValuesOk):
            print 'Number of generated SIPs: ' + str(len(retValuesOk)) + '. Check ' + self.osips
        else:
            print 'None SIPs were generated!'