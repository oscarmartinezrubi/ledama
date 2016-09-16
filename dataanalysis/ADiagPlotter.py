################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils
from ledama import config as lconfig
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.leddb.Connector import DEF_DBNAME, DEF_DBHOST
from ledama.dataanalysis.DiagPlotKey import DiagPlotKey
from ledama.dataanalysis.DiagnosticLoader import DiagnosticLoader,TIME_NORMAL, TIME_DAILY, TIME_HOUR_ANGLE, TIME_WRAP_BLANK, FREQ_NORMAL, YAXIS_TYPES
import matplotlib
matplotlib.use(lconfig.MATPLOTLIB_BACKEND)
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
from matplotlib.font_manager import FontProperties

TIME_FORMAT = "%Y_%m_%d_%H_%M_%S"

# Abstract Diagnostic plotter
class ADiagPlotter(LModule):
    def __init__(self,userName = None):
        self.userName = userName   
        if self.userName == None:
            self.userName = utils.getUserName()
        # Define the options
        options = LModuleOptions()
        options.add('inputdiag', 'i', 'DiagFile (or IDs)',)
        options.add('correlations', 'g', 'Correlations', helpmessage = ' to plot', default = '0,3')
        options.add('subplotkey', 's', 'SubPlot Key', helpmessage = ' (' + DiagPlotKey().getDescription() + '), different plot will be done for each specified key. If none is specified a single plot is done', mandatory = False)
        options.add('colorkey', 'c', 'Color key', helpmessage = '. If subplotkey is specified it must be different than the ones in colorkey', default =  'lc')
        options.add('xaxis', 'x', 'X axis', default = TIME_NORMAL,  choice =  [TIME_NORMAL, TIME_WRAP_BLANK, TIME_DAILY, TIME_HOUR_ANGLE, FREQ_NORMAL])
        options.add('xrange', 'a', 'X Range', helpmessage = '. Specify a range for the x axis. This is only possible for ' +','.join((TIME_DAILY,TIME_HOUR_ANGLE,FREQ_NORMAL))+ '. For example for ' + TIME_DAILY + ' 0,86400 or for ' + TIME_HOUR_ANGLE + '-43200,43200', mandatory=False)
        options.add('resolution', 'r', 'X axis Resolution', helpmessage = ', it is possible to specify a different resolution than the steps stored in the DB, this will speed up the plots. If X axis is time, specify in seconds. If it is frequency specify in MHz.',  mandatory = False)
        options.add('yaxis', 'y', 'Y axis', helpmessage ='. The options are: ' + '|'.join(YAXIS_TYPES) + '. Multiple values are possible (in this case specify them comma-separated)', default = 'amp,phase')
        options.add('yrange', 'e', 'Y Range', helpmessage = '. Specify, for each yaxis, the min and max. For example, for the default yaxis (amp,phase), we could use 0.,1.,0.,3.14. There is a special keyword "share" that can be used to specify that all ranges of same Y axis are shared (and auto-scaled). X axis range is always shared in all plots', mandatory = False)
        options.add('yoffset', 'f', 'Offset', helpmessage = ' in each Y axis plot between data of different color. If provided, give one value for each yaxis', mandatory = False)
        options.add('legend', 'l', 'Show legend', default = False)
        options.add('output', 'o', 'Output file', helpmessage = ', if not provided a temporal file is created', mandatory = False)
        options.add('dbname', 'w', 'DB name', default = DEF_DBNAME)
        options.add('dbuser', 'u', 'DB user', default = self.userName)
        options.add('dbhost', 'z', 'DB host', default = DEF_DBHOST)
        options.add('timeout', 't', 'Connection timeout', default = 60)
        # the information
        information = 'Plots diagnostic data into a file'
        LModule.__init__(self, options, information, )
        
        # To be assigned in each plotter
        self.diagnostickey = None
        
        # Variables required in the several plot calls
        self.keyColors = {}
        self.keyColorIndex = 0
        
    # format the X stamp
    def formatStamp(self, x, pos=None):
        return self.diagnosticLoader.formatStamp(x)

    def plot(self, fig, yDict, xDict, keys, nrows, ncols, pos, xlabel = None, ylabel= None, title = None, offset= None, shareX = None, shareY = None, showxticks = True, showyticks = True, mark = 'o', lineStyle = 'None', minx = None, maxx = None, miny = None, maxy = None, subplotenabled = False):
        
        axes = fig.add_subplot(nrows, ncols, pos, sharex=shareX, sharey = shareY)
    
        #axes = fig.gca()
        if xlabel != None:   
            axes.set_xlabel(xlabel, fontproperties=FontProperties(size="x-small"))
        if ylabel != None:
            axes.set_ylabel(ylabel, fontproperties=FontProperties(size="x-small"))
        if title != None:
            axes.set_title(title, fontproperties=FontProperties(size="x-small"))
         
        accoffset = 0.0
        for key in sorted(keys):
            if subplotenabled:
                colorKey = key[1]
            else:
                colorKey = key
            
            if colorKey not in self.keyColors:
                self.keyColors[colorKey] = utils.PCOLORS_ALT[self.keyColorIndex % len(utils.PCOLORS_ALT)]
                self.keyColorIndex += 1
            axes.plot(xDict[key], yDict[key] + accoffset, alpha=0.6, marker=mark ,linestyle = lineStyle, color=self.keyColors[colorKey], markeredgecolor = self.keyColors[colorKey], label = ' '.join(colorKey), markersize = 2.)
            if offset != None:
                accoffset += offset
        if minx != None and maxx != None:
            axes.set_xlim(minx, maxx)
        else:
            axes.autoscale(axis='x', tight=True)
        if miny != None and maxy != None:
            axes.set_ylim(miny, maxy)
        else:
            axes.autoscale(axis='y', tight=True)
        
        axes.xaxis.set_major_locator(MaxNLocator(int(80/ncols)))
        axes.xaxis.set_major_formatter(ticker.FuncFormatter(self.formatStamp))
        
        if not showxticks:
            axes.xaxis.set_visible(False)

        if not showyticks:
            axes.yaxis.set_visible(False)

        for label in axes.get_xticklabels() + axes.get_yticklabels():
            label.set_fontsize(8)
        return axes

    def process(self,inputdiag,correlations,subplotkey,colorkey,xaxis,xrange,resolution,yaxis,yrange,yoffset,legend,output,dbname,dbuser,dbhost,timeout):
        # Open the diagnostic file
        self.diagnosticLoader = DiagnosticLoader(self.diagnostickey)
        
        # Initialize the figure
        fig = plt.figure(figsize = (25,10), dpi = 75)
        # fig.clf()

        res = None
        if resolution != '':
            res = float(resolution)
            
        yaxiss = yaxis.split(',')
        numYs = len(yaxiss)
        minYs = {}
        maxYs = {}
        
        share = False
        yrelements = None
        if yrange == 'share':
            share = True  
        elif yrange != '':
            yrelements = utils.getFloats(yrange, ',')
            if len(yrelements) != 2*numYs:
                print 'ERROR: when specifying yrange please provide min,max for each yaxis'
                return 
        c = 0
        for yaxis in yaxiss:
            if yrelements != None:
                minYs[yaxis] = yrelements[c]
                c+=1
                maxYs[yaxis] = yrelements[c]
                c+=1
            else:
                minYs[yaxis] = None
                maxYs[yaxis] = None
        
        if yoffset != '':
            yoffsets = utils.getFloats(yoffset, ',')
            if len(yoffsets) != numYs:
                    print 'ERROR: when specifying yoffsets please provide one for each yaxis'
                    return
        else:
            yoffsets = []
            for yaxis in yaxiss:
                yoffsets.append(0.)    
        (xs, yss, yaxislabels, subPlotsKeys) = self.diagnosticLoader.load(inputdiag, subplotkey, colorkey, xaxis, res, yaxiss, utils.getElements(correlations), dbname, dbuser, dbhost, timeout)
    
        minX = None
        maxX = None
        if xrange != '':
            if xaxis not in (TIME_DAILY,TIME_HOUR_ANGLE,FREQ_NORMAL):
                print 'ERROR: xrange only works with ' +','.join((TIME_DAILY,TIME_HOUR_ANGLE,FREQ_NORMAL))
                return
            (minX,maxX) = utils.getFloats(xrange, ',')
        else:
            for key in xs:
                minXs = xs[key].min()
                maxXs = xs[key].max()
                if minX == None or (minX > minXs):
                    minX = minXs
                if maxX == None or (maxX < maxXs):                
                    maxX = maxXs

        # Define the labels depending on each case
        if xaxis == TIME_NORMAL:
            xLabel = 'UTC Time'
        elif xaxis == TIME_DAILY:
            xLabel = 'UTC Daily Time'
        elif xaxis == TIME_WRAP_BLANK:
            xLabel = 'UTC Time (wrap)'
        elif xaxis == TIME_HOUR_ANGLE:
            xLabel = 'Hour Angle'
        elif xaxis == FREQ_NORMAL:
            xLabel = 'Freq. (MHz)'

        print 'Plotting...'
        
        if subPlotsKeys != None:
            (nrows,ncols) = utils.getNM(len(subPlotsKeys)*numYs, numYs)
            sharedPlots = {}
            sharedXPlot = None
            sharedYPlot = None
            for subPlotIndex in range(len(subPlotsKeys)):
                subPlotKey = sorted(subPlotsKeys.keys())[subPlotIndex]
                keys = subPlotsKeys[subPlotKey]
                for j in range(len(keys)):
                    keys[j] = (subPlotKey, keys[j])
                posY0 = utils.getPosition(subPlotIndex, nrows, ncols, numYs) + 1 # we should start at 1, not 0
                showYTicks = True
                if minYs[yaxis] != None and not (((posY0-1) % ncols == 0) and ((posY0-1) >= ((nrows*ncols) - numYs*ncols))):
                    showYTicks = False
                yi = 0  
                for yaxis in yaxiss:
                    xL = None
                    y0L = None
                    if (posY0-1) % ncols == 0:
                        xL = xLabel
                        y0L = yaxislabels[yaxis]
                    showXTicks = False
                    posYi = posY0 + (yi*ncols)
                    if (posYi-1) == ((nrows*ncols) - ncols):
                        showXTicks = True    
                    if len(sharedPlots):
                        sharedXPlot = sharedPlots.values()[0]
                    sharedYPlot = None
                    if share:
                        sharedYPlot = sharedPlots.get(yaxis)
                    title = None
                    if yi == 0:
                        title = ' '.join(subPlotKey)
                    sharedPlots[yaxis] = self.plot(fig, yss[yaxis], xs, keys, nrows, ncols, posYi, xL, y0L, title, offset = yoffsets[yi], shareX = sharedXPlot, shareY = sharedYPlot, showxticks = showXTicks, showyticks = showYTicks, minx = minX, maxx = maxX,miny = minYs[yaxis], maxy = maxYs[yaxis], subplotenabled = True)
                    yi += 1
        else:
            yi = 0
            (nrows,ncols) = (numYs,1)
            sharedPlot = None
            for yaxis in yaxiss:
                sharedPlot = self.plot(fig, yss[yaxis], xs, xs.keys(), nrows, 1, yi+1, xLabel, yaxislabels[yaxis], offset = yoffsets[yi], shareX = sharedPlot, minx = minX, maxx = maxX, miny = minYs[yaxis], maxy = maxYs[yaxis])    
                yi += 1
                
        if legend:
            fig.gca().legend(prop=FontProperties(size="x-small"), markerscale=1.)
        
        fig.autofmt_xdate()  
        try:
            plt.tight_layout(pad=0.2, h_pad=0.12, w_pad=0.12)
        except:
            fig.subplots_adjust(top=0.97)
            fig.subplots_adjust(right=0.98)
            fig.subplots_adjust(left=0.03)
            fig.subplots_adjust(bottom=0.09)
            fig.subplots_adjust(hspace=0.06)
            fig.subplots_adjust(wspace=0.07)

        if output == '':
            output = utils.getCurrentTimeStamp(TIME_FORMAT) + 'tempImage.png'
        print 'Image created: ' + output
        fig.savefig(output)    
