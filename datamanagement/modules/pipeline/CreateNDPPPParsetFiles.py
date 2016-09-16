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
    
NDPPP_EXTENSION = '.dppp'
EXTENSION = '.parset'
    
class CreateNDPPPParsetFiles(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('reffile', 'i', 'Input RefFile')
        options.add('parset', 't', 'Input template NDPPP parset file')
        options.add('oparset', 'o', 'Output NDPPP parset files folder')
        options.add('ocommon', 'c', 'Output data path',mandatory=False,helpmessage='. The path of each output MSP will be: [output data path]/[MS name].dppp. It is possible to use XXXXX or XXXX_XXXXX in the paths, they will be replaced for the proper characters in each case')
        options.add('numprocessors', 'p', 'Simultaneous processes per node', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)        
        # the information
        information = 'Create NDPPP parset files'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    # Make the parset file for the measurement set indicated in what
    # Note that in msin we always add /net/nodexxx
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        (inputms,outputms,outputParsetFilePath) = what
        ofile = open(outputParsetFilePath, "w")
        # we add msin and msout with our data in the beginning
        ofile.write('msin = /net/' + node + inputms + '\n')
        if inputms != outputms:
            ofile.write('msout = ' + outputms + '\n')
        else:
            ofile.write('msout = ' + '\n')
        for line in self.inParsetFileLines:
            # we write the rest of the lines exactly like in the original file
            # except the lines related to msin and msout
            if line.startswith('msin.') or line.startswith('msout.') or (not line.startswith('msin') and not line.startswith('msout')):
                ofile.write(line + '\n')
        ofile.close()
        return (inputms, outputParsetFilePath)
        
    def process(self, reffile, parset, oparset, ocommon, numprocessors, numnodes):
        # Check if file exists
        if not os.path.isfile(parset):
            print 'Error: ' + parset + ' does not exists'
            return 
        self.inParsetFileLines = (open(parset, "r")).read().split('\n')
        parsetsOutPath = utils.formatPath(oparset)
        commonOutPath = utils.formatPath(ocommon)
        
        os.system('mkdir -p ' + parsetsOutPath )
        referenceFile = ReferenceFile(reffile)
        whats = []
        for i in range(len(referenceFile.absPaths)):
            absPath = referenceFile.absPaths[i]
            msName = msoperations.getMeasurementSetName(absPath)
            lds = msoperations.getLDSName(absPath)[1:]
            if commonOutPath == '' or commonOutPath == msoperations.getParentPath(absPath):
                outputms = absPath
            else:
                if commonOutPath.count('XXXXX'):
                    if commonOutPath.count('XXXX_XXXXX'):
                        year = msoperations.getYear(absPath)
                        if year == None:
                            print 'Error replacing XXXX: Can not get the year from the initial path in ' + absPath
                            return
                        outputms = commonOutPath.replace('XXXXX', lds).replace('XXXX',year)
                    else:
                        outputms = commonOutPath.replace('XXXXX', lds)
                    if (not msoperations.isRaw(msName)) and (commonOutPath.count('pipeline') == 0) and (commonOutPath.count('lofareor') != 0):
                        # add pipeline sub-folder
                        outputms = commonOutPath.replace('lofareor','lofareor/pipeline')      
                else:  
                    outputms = commonOutPath
                outputms +=  '/' + msName
            # If the output path does not end with .dppp we add it
            if not outputms.endswith(NDPPP_EXTENSION):
                outputms += NDPPP_EXTENSION
            
            whats.append((absPath, outputms, parsetsOutPath + '/' + msName + EXTENSION))
    
        (retValuesOk, retValuesKo) = td.distribute(referenceFile.nodes, whats, self.function, numprocessors, numnodes)
        td.showKoFirst(retValuesKo) 
        if len(retValuesOk):
            print str(len(retValuesOk)) + ' NDPPP parset files were created. Check ' + parsetsOutPath