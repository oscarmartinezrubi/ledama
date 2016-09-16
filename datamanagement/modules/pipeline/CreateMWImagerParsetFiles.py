################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.datamanagement.modules.pipeline.CreateGDSFiles import EXTENSION as EXTENSION_GDS
    
EXTENSION = '.parset'

class CreateMWImagerParsetFiles(LModule):    
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('gdss', 'i', 'Input GDS files folder')
        options.add('parset', 't', 'Input template MWImager parset file')
        options.add('oparset', 'o', 'Output MWImager parset files folder')
        
        # the information
        information = 'Create MWImager parset files'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   
        
    def writeParsetFile(self, gdsFile):
        outfile = self.parsetsOutPath + '/' + gdsFile.split('/')[-1].split('.')[0] + EXTENSION
        ofile = open(outfile, "w")
        for line in self.inParsetFileLines:
            if line.count('dataset'):
                ofile.write('dataset = ' + gdsFile + '\n')
            else:
                ofile.write(line + '\n')
        ofile.close()
        print outfile

    def process(self, gdss, parset, oparset):
        if not os.path.isfile(parset):
            print 'Error: ' + parset + ' does not exists'
            return
        self.inParsetFileLines = (open(parset, "r")).read().split('\n')
        self.parsetsOutPath = oparset
        os.system('mkdir -p ' + self.parsetsOutPath )
        
        gdsInputPath = utils.formatPath(gdss)
        if not os.path.isdir(gdsInputPath):
            print 'Input path does not exist'
            return 

        for element in os.listdir(gdsInputPath):
            if element.endswith(EXTENSION_GDS):
                self.writeParsetFile(gdsInputPath + '/' + element)