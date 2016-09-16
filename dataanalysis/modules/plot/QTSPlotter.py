################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import QTS_KEY
from ledama.dataanalysis.ADiagPlotter import ADiagPlotter

class QTSPlotter(ADiagPlotter):
    def __init__(self,userName = None):
        ADiagPlotter.__init__(self, userName)
        self.diagnostickey = QTS_KEY