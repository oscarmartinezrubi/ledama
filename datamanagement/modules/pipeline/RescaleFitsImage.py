################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from pyrap.images import image
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions

class RescaleFitsImage(LModule):
    def __init__(self,userName = None):
        # Define the options
        options = LModuleOptions()
        options.add('fits', 'i', 'Input FITS file')
        options.add('output', 'o', 'Output FITS file', helpmessage=' for the generated scale file')
        options.add('factor', 'f', 'Scale factor', default = 2)
        # the information
        information = 'Rescale a FITS image'
        
        # Initialize the parent class
        LModule.__init__(self, options, information)   

    def process(self, fits, output,factor):
        image(fits+'*'+str(factor)).tofits(output, velocity=False, bitpix=-32)
    
   

