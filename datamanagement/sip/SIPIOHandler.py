#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.datamanagement.sip import sipapi

# SIP file suffix (SIP_FILE_PATH = MS_FILE_PATH + SIP_SUFFIX)
SIP_SUFFFIX = '.SIP.XML'
DATE_HEADER_PREFIX = '<!--DATE:'
DATE_HEADER_SUFFIX = '-->'

class SIPIOHandler:
    """SIP IO handler using the sipapi generated from generateSD and LTA-SIP XSD"""

    def __init__(self, absPath = None):
        """ Initialize the SIP IO Handler and read a SIP if absPath is specified """
        self.absPath = None
        self.header = None
        self.sip = None
        if absPath != None:
            self.read(absPath)
            
    def getSIPPath(self, msAbsPath):
        """ Get the SIP file path from the related MS file path """
        return msAbsPath + SIP_SUFFFIX
    
    def getSIP(self):
        return self.sip

    def read(self, absPath):
        """ Read the SIP contents
                - Creates the DataStructure using the SIP API
        """
        self.absPath = absPath
        sipFile = open(self.absPath, 'r')
        self.sipFileContent = sipFile.read()
        sipFile.close()
        self.sip = sipapi.parse(absPath)
        
        
    def write(self, outputPath):
        """ Write the SIP in the specified path. 
            It uses the first tag as extracted from the original SIP.
            - Gets the tag header from the original SIP 
                (the XML extractor does not correctly sets the first tab, so we
                copy-paste it form the original SIP) 
        """
        tempOutPath= outputPath + 'TEMP'
        self.sip.export(open(tempOutPath,'w'),0)
        tempSipFile = open(tempOutPath, 'r')
        tempSipFileContent = tempSipFile.read()
        tempSipFile.close()
        header = self.sipFileContent[:self.sipFileContent.index('<sipGeneratorVersion>')]
        tempSipFileContent = tempSipFileContent.replace('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" ', '')
        tempSipFileContent = header + tempSipFileContent[tempSipFileContent.index('<sipGeneratorVersion>'):]
        tempSipFileContent = tempSipFileContent.replace('</LTASip>', '</sip:ltaSip>')
        oFile = open(outputPath, 'w')
        oFile.write(tempSipFileContent)
        oFile.close()
        os.system('chmod 777 ' + outputPath)
        os.system('rm ' + tempOutPath)