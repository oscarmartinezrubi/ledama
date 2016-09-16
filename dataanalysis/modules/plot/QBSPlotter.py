################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import QBS_KEY
from ledama.dataanalysis.ADiagPlotter import ADiagPlotter

class QBSPlotter(ADiagPlotter):
    def __init__(self,userName = None):
        ADiagPlotter.__init__(self, userName)
        self.diagnostickey = QBS_KEY