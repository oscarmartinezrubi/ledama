#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os,sys
import ledama.utils as utils
import ledama.msoperations as msoperations

# This class defines the writing and reading of the LOFARDataSet reference file
# compatible with LEDDB

class ReferenceFile:
    EXTENSION = '.ref'
    CREATED = '# Created on: ' 
    DATASELECTION_LINE_PREFIX = '# LEDDB QUERY: '
    
    # Constructor. If only the filePath is passed we read the parameters from it
    # If more arguments are passed we assume them 
    def __init__(self, filePath, dataSelection = None, absPaths = None, refFreqs = None, sizes = None, nodes = None, beamIndexes = None):   
        
        self.filePath = filePath
        
        if absPaths == None:
            self.read()
        else:
            self.dataSelection = dataSelection
            self.absPaths = absPaths
            self.refFreqs = refFreqs
            self.sizes = sizes
            self.nodes = nodes
            self.beamIndexes = beamIndexes
            self.createdDate = utils.getCurrentTimeStamp()
    
    # Get a header field
    def getHeaderField(self, lines, field):
        for line in lines:
            if line.count(field):
                return line.replace(field,'').strip()
        return 'unknown'  
        
    # Read the reffile to get the information
    def read(self):
        # Open the file
        try:
            lines = open(self.filePath, "r").read().split('\n')
        except:
            raise Exception('Error: file not found')
        
        self.createdDate = self.getHeaderField(lines, self.CREATED)
        
        # Get the MSP lines (until we reach the picked area of the file)
        self.absPaths=[]
        self.refFreqs=[]
        self.sizes=[]
        self.nodes=[]
        self.beamIndexes=[]
        self.dataSelection = None
        for line in lines:
            if (line == ''):
                continue 
            elif line.count(self.DATASELECTION_LINE_PREFIX):
                # This should be the last line
                self.dataSelection = line.replace(self.DATASELECTION_LINE_PREFIX, '')
                break
            elif (line[0] != '#'):
                fields = line.split()
                if len(fields) >=4:
                    self.absPaths.append(fields[0])
                    self.refFreqs.append(fields[1])
                    try:
                        size = int(fields[2])
                    except:
                        size = '?'
                    self.sizes.append(size)
                    self.nodes.append(fields[3])
                    if len(fields) == 5:
                        # it is a new refFile
                        try:
                            beamIndex = int(fields[4])
                        except:
                            beamIndex = '?'
                    else:
                        # it is an old refFile
                        beamIndex = msoperations.getBeamIndex(fields[0])
                    self.beamIndexes.append(beamIndex)

    # Write the refFile in the filePath
    def write(self):
        
        # Check goodness of the arguments passed
        if (len(self.absPaths) != len(self.refFreqs)) or (len(self.absPaths) != len(self.sizes)) or (len(self.absPaths) != len(self.nodes)) or (len(self.absPaths) != len(self.beamIndexes)):
            raise Exception('Error: Different length in absPaths, refFreqs, sizes, nodes, beamIndexes')
        
        if self.filePath == None:
            reffile = sys.stdout
        else:
            # Check for already existing output files
            if os.path.isfile(self.filePath):
                raise Exception('Error: ' + self.filePath + ' already exists')
            
            reffile = open(self.filePath, "wb")
        
        # Write the Headers
        reffile.write(self.CREATED + ' ' + self.createdDate + "\n")
        reffile.write('# Number of MSPs: ' + str(len(self.absPaths)) + "\n")
        try:
            reffile.write('# Total size (MB): ' + str(sum(self.sizes)) + "\n")
        except:
            reffile.write('# Total size (MB): ? \n')
        reffile.write('# Frequency range (MHz): ' + self.getFreqRange() + "\n\n")
        for i in range(len(self.absPaths)):
            reffile.write(self.absPaths[i] + ' ' + str(self.refFreqs[i]) + ' ' + str(self.sizes[i]) + ' ' +  self.nodes[i]  + ' ' +  str(self.beamIndexes[i]) + "\n")
        
        if self.dataSelection != None:
            # We write the LEDDB query statement
            reffile.write('\n\n' + self.DATASELECTION_LINE_PREFIX + self.dataSelection)
        
        if self.filePath != None:
            # Close and Give permission to created file (if created)
            reffile.close()
            os.system('chmod 777 ' + self.filePath)
        
    # Combine current refFile with the one passed as parameter, when combinig 
    # refFiles the dataSelection is set to None
    def combine(self, refFileToCombine):
        self.dataSelection = None
        self.absPaths.extend(refFileToCombine.absPaths)
        self.refFreqs.extend(refFileToCombine.refFreqs)
        self.sizes.extend(refFileToCombine.sizes)
        self.nodes.extend(refFileToCombine.nodes)
        self.beamIndexes.extend(refFileToCombine.beamIndexes)
        self.createdDate = utils.getCurrentTimeStamp()
    
    # Get minimum and maximum frequency
    def getFreqRange(self):
        minFreq = None
        maxFreq = None
        for freq in self.refFreqs:
            if type(freq) == str:
                try:
                    freq = float(freq.replace('MHz', ''))
                except:
                    freq = None
            if freq != None and type(freq) == float:
                if maxFreq == None or freq > maxFreq:
                    maxFreq = freq
                if minFreq == None or freq < minFreq:
                    minFreq = freq
        return str(minFreq) + ' - ' + str(maxFreq)
    
    def validateSizes(self):
        for size in self.sizes:
            if type(size) != int:
                raise Exception('Some sizes are not available!')