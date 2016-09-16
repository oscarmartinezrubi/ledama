################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import GAIN_KEY
from ledama.dataanalysis.ADiagPlotter import ADiagPlotter

class GainPlotter(ADiagPlotter):
    def __init__(self,userName = None):
        ADiagPlotter.__init__(self, userName)
        self.diagnostickey = GAIN_KEY