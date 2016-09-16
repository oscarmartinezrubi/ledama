#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import ledama.utils as utils
import os, sys

# This class defines the writing and reading of the LDSBP Update file
# This file is used to update the LDSBP info regarding the applied processing pipeline steps
# It has a format as in the example in next lines

#        # GENERAL_DESCRIPTION AND MAIN_USER: texts
#        GENERAL_DESCRIPTION = my description of the processing
#        MAIN_USER = martinez

#        # PIPELINE DESCRIPTION: True, False or None
#        IS_FLAGGED = True
#        IS_AVERAGED = True
#        IS_CALIBRATED = True
#        IS_DIRECTION_DEPENDENT_CAL = True
#        IS_DIRECTION_INDEPENDENT_CAL = True
#        
#        #PIPELINE STEPS: Define only the steps that differentiate this data from its previous version (LEDDB version)
#        #(Both DESCRIPTION and FILES are optional)
#
#        ORDER = 0
#        APP_NAME = NDPPP
#        DESCRIPTION = my ndppp description
#        FILES = mypathtoafileforndppp,mypathtoanotherfileforndppp
#
#        ORDER = 1
#        APP_NAME = BBS
#        DESCRIPTION = my bbs description
#        FILES = mypathtoafileforbbs

class LDSBPUpFile:

    def __init__(self, filePath, description = None, mainUser = None, flagged = None, averaged = None, calibrated = None,
                 dirDepCalibrated = None, dirIndepCalibrated = None, appOrders = [], appNames = [], appDescriptions = [], appFiles = [], appCommented = []):   
        """Constructor. If only the filePath is passed we read the parameters from it.
        If more arguments are passed we assume them there is no file to read, it is just the desired output file name"""
        self.filePath = filePath

        self.description = description
        self.mainUser = mainUser
        self.flagged = flagged
        self.averaged = averaged
        self.calibrated = calibrated
        self.dirDepCalibrated = dirDepCalibrated
        self.dirIndepCalibrated = dirIndepCalibrated
        self.appOrders = appOrders
        self.appNames = appNames            
        self.appDescriptions = appDescriptions
        self.appFiles = appFiles
        self.appCommented = appCommented

    def parseBoolean(self, booleanString):
        """Return the boolean from the string or None if it is not recognized"""
        try:
            b = utils.booleanStringToBoolean(booleanString)
        except:
            b = None
        return b    
    
    def read(self):
        """Read the upfile to get the information"""
        # Open the file
        try:
            lines = open(self.filePath, "r").read().split('\n')
        except:
            raise Exception('Error: file not found')
        
        for line in lines:
            if not line.count('#'):
                if line.count('GENERAL_DESCRIPTION'):
                    self.description = line.split('=')[-1].strip()
                elif line.count('MAIN_USER'):
                    self.mainUser = line.split('=')[-1].strip()
                elif line.count('IS_FLAGGED'):
                    self.flagged = self.parseBoolean(line.split('=')[-1].strip())
                elif line.count('IS_AVERAGED'):
                    self.averaged = self.parseBoolean(line.split('=')[-1].strip())
                elif line.count('IS_CALIBRATED'):
                    self.calibrated = self.parseBoolean(line.split('=')[-1].strip())
                elif line.count('IS_DIRECTION_DEPENDENT_CAL'):
                    self.dirDepCalibrated = self.parseBoolean(line.split('=')[-1].strip())
                elif line.count('IS_DIRECTION_INDEPENDENT_CAL'):
                    self.dirIndepCalibrated = self.parseBoolean(line.split('=')[-1].strip())
                elif line.count('ORDER'):
                    self.appOrders.append(int(line.split('=')[-1].strip()))
                    self.appCommented.append(False)
                elif line.count('APP_NAME'):
                    self.appNames.append(line.split('=')[-1].strip())
                elif line.count('DESCRIPTION'):
                    self.appDescriptions.append(line.split('=')[-1].strip())
                elif line.count('FILES'):
                    self.appFiles.append(line.split('=')[-1].strip())

    def validate(self):
        """Validate the contents"""
        if (len(self.appNames) != len(self.appOrders)) or (len(self.appNames) != len(self.appDescriptions)) or (len(self.appNames) != len(self.appFiles)) or (len(self.appNames) != len(self.appCommented)):
            print 'Lenghts are: ' + ','.join([str(len(self.appNames)), str(len(self.appOrders)), str(len(self.appDescriptions)), str(len(self.appFiles)), str(len(self.appCommented))])
            raise Exception('Error: Different length in appNames, appOrders, appDescriptions, appFiles, appCommented')
#        if type(self.flagged) != bool:
#            raise Exception('IS_FLAGGED must be True or False')
#        if type(self.averaged) != bool:
#            raise Exception('IS_AVERAGED must be True or False')
#        if type(self.calibrated) != bool:
#            raise Exception('IS_CALIBRATED must be True or False')
#        if type(self.dirDepCalibrated) != bool:
#            raise Exception('IS_DIRECTION_DEPENDENT_CAL must be True or False')
#        if type(self.dirIndepCalibrated) != bool:
#            raise Exception('IS_DIRECTION_INDEPENDENT_CAL must be True or False')
        return True
        
    def write(self):
        """Write the contents in file in the filePath"""
        if self.filePath == None:
            upfile = sys.stdout
        else:
            # Check for already existing output files
            if os.path.isfile(self.filePath):
                raise Exception('Error: ' + self.filePath + ' already exists')
            
            upfile = open(self.filePath, "wb")
        
        # Write the Headers
        upfile.write('# GENERAL_DESCRIPTION AND MAIN USER: texts' + '\n')
        upfile.write('GENERAL_DESCRIPTION = ' + str(self.description) + '\n')
        upfile.write('MAIN_USER = ' + str(self.mainUser) + '\n')
        upfile.write('\n')
        upfile.write('# PIPELINE DESCRIPTION: booleans' + '\n')
        upfile.write('IS_FLAGGED = ' + str(self.flagged) + '\n')
        upfile.write('IS_AVERAGED = ' + str(self.averaged) + '\n')
        upfile.write('IS_CALIBRATED = ' + str(self.calibrated) + '\n')
        upfile.write('IS_DIRECTION_DEPENDENT_CAL = ' + str(self.dirDepCalibrated) + '\n')
        upfile.write('IS_DIRECTION_INDEPENDENT_CAL = ' + str(self.dirIndepCalibrated) + '\n')
        upfile.write('\n')
        upfile.write('#PIPELINE STEPS: Define only the steps that differentiate this data from its previous version (LEDDB version)' + '\n')
        upfile.write('#(Both DESCRIPTION and FILES are optional)' + '\n')
        if not len(self.appNames):
            upfile.write('\n')
            upfile.write('#NO APPS EXTRACTED FROM HISTORY OR LEDDB. Those two apps are here as an example.' + '\n')
            upfile.write('#ORDER = 0' + '\n')
            upfile.write('#APP_NAME = myAppExample ' + '\n')
            upfile.write('#DESCRIPTION = my app description ' + '\n')
            upfile.write('#FILES = mypathtoafileformyapp,mypathtoanotherfileformyapp' + '\n')
            upfile.write('\n')
            upfile.write('#ORDER = 1' + '\n')
            upfile.write('#APP_NAME = myAnotherAppExample ' + '\n')
            upfile.write('#DESCRIPTION = ' + '\n')
            upfile.write('#FILES = ' + '\n')
        
        for appIndex in range(len(self.appNames)):
            extra = ''
            if self.appCommented[appIndex]:
                extra = '#'
            upfile.write('\n')
            upfile.write(extra + 'ORDER = ' + str(self.appOrders[appIndex]) + '\n')
            upfile.write(extra + 'APP_NAME = ' + self.appNames[appIndex] + '\n')
            upfile.write(extra + 'DESCRIPTION = ' + self.appDescriptions[appIndex] + '\n')
            upfile.write(extra + 'FILES = ' + self.appFiles[appIndex] + '\n')
              
        if self.filePath != None:
            # Close and Give permission to created file (if created)
            upfile.close()
            os.system('chmod 777 ' + self.filePath)
