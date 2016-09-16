################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama.leddb.Naming import QFS_KEY
from ledama.dataanalysis.ADiagPlotter import ADiagPlotter

class QFSPlotter(ADiagPlotter):
    def __init__(self,userName = None):
        ADiagPlotter.__init__(self, userName)
        self.diagnostickey = QFS_KEY