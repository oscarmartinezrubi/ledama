################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import LDS, CENTFREQ, STATION, CORR, DIRRA, DIRDEC,\
    NAME, STATION2, QKIND

# A class to define what is plotted
class DiagPlotKey:
    # Initialize with the several attributes that can define a diag. plot
    #    names is a list of the attributes, example [LDS, SBINDEX] or a string
    def __init__(self, names = None):
        if names != None:
            if type(names) == list:
                self.names = names
            else:
                self.names = []
                if ('l' in names):
                    self.names.append(LDS)
                if ('f' in names):
                    self.names.append(CENTFREQ)
                if ('s' in names):
                    self.names.append(STATION)
                if ('c' in names):
                    self.names.append(CORR)
                if ('r' in names):
                    self.names.append(DIRRA)
                if ('d' in names):
                    self.names.append(DIRDEC)
                if ('q' in names):
                    self.names.append(NAME)
                if ('b' in names):
                    self.names.append(STATION2)
                
    def getDescription(self):
        return 'l:' + LDS + ', f:' + CENTFREQ + ', s:' + STATION + ', c:' + CORR + ', r:' + DIRRA + ', d:' + DIRDEC + ', q:' + QKIND + ', b:' + STATION2
     
    # Get a tuple key with the only the values of the enabled attributes
    def getKey(self, attDict):
        key = []
        for name in self.names:
            key.append(str(attDict[name]))
        return tuple(key)
    
    # Get the attDict to be usde by getKey
    def getAttDict(self, lds, centFreq, station, corr, dirRA, dirDec, station2, qKind):
        return {LDS:lds, CENTFREQ:centFreq, STATION: station, CORR: corr, DIRRA:dirRA, DIRDEC:dirDec, STATION2: station2, NAME: qKind}
    
    # Compare two diagPlotKey, the must contain different att names
    def compare(self, diagPlotKey):
        for name in self.names:
            if name in diagPlotKey.names:
                return False
        return True
    
    # Check that the contained names in current key are also contained in the specified namesvalidate
    def validate(self, namesvalidate):
        for name in self.names:
            if (name != CORR) and (name not in namesvalidate):
                return False
        return True
