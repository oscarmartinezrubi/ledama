################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama import utils
from ledama import msoperations
from ledama import tasksdistributor as td
from ledama.ReferenceFile import ReferenceFile
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

EXTENSION = '.imaging.parset'

class CreateCASAImagerParsetFiles(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parset', 't', 'Input template CASAImager parset file')
        options.add('oparset', 'o', 'Output CASAImager parset files folder')
        options.add('oextension', 'e', 'Output files extension',helpmessage='. The produced files will be stored in a folder defined in LaunchCASAImager, but in this point you can define the common file name. The produced files have a file name as [MS name].[oextension].*', default='.img.ver0')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)      
        # the information
        information = 'Create CASAImager parset files'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Make the parset file for the measurement set indicated in what
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        (inputms,node,imagename,outputParsetFilePath) = what
        ofile = open(outputParsetFilePath, "w")
        for line in self.inParsetFileLines:
            if line.count('vis'):
                ofile.write('vis="/net/' + node + inputms + '"\n')
            elif line.count('imagename'):
                ofile.write('imagename="' + imagename + '"\n')
            else:
                ofile.write(line + '\n')
        ofile.close()
        return (inputms, outputParsetFilePath)
    
    # Get the VDS file name (it is the MS name + .vds)
    def getCASAImagerParsetFileName(self, absPath):
        return msoperations.getMeasurementSetName(absPath) + EXTENSION
        
    def process(self, reffile, parset, oparset, oextension, numprocessors, numnodes):
        # Check if file exists
        if not os.path.isfile(parset):
            print 'Error: ' + parset + ' does not exists'
            return 
        self.inParsetFileLines = (open(parset, "r")).read().split('\n')
        parsetsOutPath = utils.formatPath(oparset)
        os.system('mkdir -p ' + parsetsOutPath )
        referenceFile = ReferenceFile(reffile)
        whats = []
        for i in range(len(referenceFile.absPaths)):
            absPath = referenceFile.absPaths[i]
            whats.append((absPath, referenceFile.nodes[i], msoperations.getMeasurementSetName(absPath) + oextension, parsetsOutPath + '/' + self.getCASAImagerParsetFileName(absPath)))
    
        # Run it
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, whats, self.function, numprocessors, numnodes)
        td.showKoFirst(retValuesKo) 
        if len(retValuesOk):
            print str(len(retValuesOk)) + ' CASA imager parset files were created. Check ' + parsetsOutPath