################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os
from ledama.LModule import LModule
from ledama.LModuleOptions import LModuleOptions
from ledama.PrettyTable import PrettyTable
from ledama import utils
from ledama import config as lconfig
from ledama.ReferenceFile import ReferenceFile
from ledama import tasksdistributor as td
from ledama import msoperations
import pyrap.tables as pt
import numpy

def processMS(absPath, output,overwrite,stats,column,timeslots,channels,antennas, baselines, correlations, wrap, flag, colflag, stokes, autocorr,operation, acc):
    overWrite = utils.booleanStringToBoolean(overwrite)
    showFlags = utils.booleanStringToBoolean(flag)
    acc = int(acc)
    showAutocorr = utils.booleanStringToBoolean(autocorr)
    flagCol = colflag
    timeslots = timeslots.split(',')
    if len(timeslots) != 2:
        print 'Error: Timeslots format is start,end'
        return
    for i in range(len(timeslots)): timeslots[i] = int(timeslots[i])
    
    antToPlot = []
    basesToPlot = []
    if baselines == '':
        antToPlotSpl = antennas.split(',')
        for i in range(len(antToPlotSpl)):
            tmpspl = antToPlotSpl[i].split('..')
            if len(tmpspl) == 1:
                antToPlot.append(int(antToPlotSpl[i]))
            elif len(tmpspl) == 2:
                for j in range(int(tmpspl[0]),int(tmpspl[1])+1):
                    antToPlot.append(j)
            else:
                print 'Error: Could not understand antenna list.'
                return
    else:
        basesToPlotSpl = baselines.split(',')
        for i in range(len(basesToPlotSpl)):
            tmpspl = basesToPlotSpl[i].split('-')
            if len(tmpspl) == 2:
                basesToPlot.append((int(tmpspl[0]), int(tmpspl[1])))
                antToPlot.append(int(tmpspl[0]))
                antToPlot.append(int(tmpspl[1]))
            else:
                print 'Error: Could not understand baseline list.'
                return
        
    corrs = correlations.split(',')
    for i in range(len(corrs)):
        corrs[i] = int(corrs[i])
    
    convertStokes = utils.booleanStringToBoolean(stokes)  
    if operation != '':
        operation = int(operation)
        if convertStokes:
            print 'Error: Stokes conversion is not compatible with special operations'
            return
    
    channels = channels.split(',')
    if len(channels) != 2:
        print 'Error: Channels format is start,end'
        return
    for i in range(len(channels)): channels[i] = int(channels[i])
    if channels[1] == -1:
        channels[1] = None # last element even if there is only one
    else:
        channels[1] += 1
    doUnwrap = utils.booleanStringToBoolean(wrap)

    # open the main table and print some info about the MS
    t = msoperations.getTable(absPath)
    
    # we get the first and last time, assuming the data is sorted by TIME
    times = pt.taql('select DISTINCT TIME from ' + absPath)
    if timeslots[1] == -1:
        timeslots[1] = times.nrows() -1
    
    for i in range(len(timeslots)):
        if (timeslots[i] < 0) or (timeslots[i] > len(times)):
            print 'Error: specified timeslots out of valid range, number samples is ' + str(len(times))
            return
    
    # Station names
    antList = msoperations.getStations(t)
    if len(antToPlot)==1 and antToPlot[0]==-1:
        antToPlot = range(len(antList))

    freq = msoperations.getCentralFrequency(t)
    
    statslist = stats.split(',')
    complexcoordinates = []
    statparams = []
    for stat in statslist:
        if stat.count('_'):
            sfields = stat.split('_')
            if len(sfields) == 2:
                complexcoordinates.append(sfields[0])
                statparams.append(sfields[1])
    if len(complexcoordinates) == 0:
        print 'Error: check specified stats format'
        return
    for complexcoord in complexcoordinates:
        if complexcoord not in ('amp','phase','real','imag','phaserate'):
            print 'Error: check specified stats format'
            return
    for statparam in statparams:
        if statparam not in ('mean','median','std'):
            print 'Error: check specified stats format'
            return
    compCoordDict = utils.getIndexesDictionary(complexcoordinates)
    
    header = ['#sbfreq','ant1', 'ant2', 'ant1Name', 'ant2Name', 'pol', 'num', 'numflag']
    header.extend(statslist)
    os.system('mkdir -p ' + output)
    ofilename = output + '/' + msoperations.getMeasurementSetName(absPath) + '.stats'
    if os.path.isfile(ofilename):
        if overWrite:
            os.system('rm ' + ofilename)
        else:
            print 'Error: ' + ofilename + ' already exists! (maybe you want to use option -d)'
            return
    outputfile = open(ofilename, "w")
    
    pTable = PrettyTable(header)
    pTable.border = False
    # select by time from the beginning, and only use specified antennas
    tsel = t.query('TIME >= %f AND TIME <= %f AND ANTENNA1 IN %s AND ANTENNA2 IN %s' % (times.getcell('TIME',timeslots[0]),times.getcell('TIME',timeslots[1]),str(antToPlot),str(antToPlot)))

    if convertStokes:
        polLabels = ['I','Q','U','V']
    else:
        polLabels = ['XX','XY','YX','YY']
    
    # prefaxis is to make samples integrated over the f axis after the cut
    prefaxis = 0

    # Now we loop through the baselines
    for tpart in tsel.iter(["ANTENNA1","ANTENNA2"]):
        ant1 = tpart.getcell("ANTENNA1", 0)
        ant1Name = antList[ant1]
        ant2 = tpart.getcell("ANTENNA2", 0)
        ant2Name = antList[ant2]
        # If there is a baseline list, we check if ant1 and ant2 
        if len(basesToPlot):
            plotBaseline = False
            for baseline in basesToPlot:
                if ((ant1,ant2) == baseline) or ((ant2,ant1) == baseline):
                    plotBaseline = True
                    break
            if not plotBaseline:
                continue
        else:
            if ant1 not in antToPlot or ant2 not in antToPlot: 
                continue
            if ant1 == ant2 and not showAutocorr:
                continue
        
        # Get the 3D cut data [time][freq][pol]
        cutData = msoperations.get3DCutData(tpart, column, showFlags, flagCol, channels, convertStokes)
        
        if cutData is None: # This baseline must be empty, go to next one
            print 'No good data on baseline %s - %s' % (ant1Name,ant2Name)
            continue
        
        if operation != 0: # A special operation of the corrs is required
            
            # we get the integrated data 
            intData = msoperations.getAvgDataOperation(cutData, operation, prefaxis)
            statsresults = msoperations.getStats(intData, compCoordDict, statparams, doUnwrap)
                
            # To know the num and nummasked we do not need the operation itself,
            # only to know how the combination of the corrs affected the
            # masks (but we need to do this using the non integrated data)
            if operation == 1:
                label = 'XX-YY'
                cData = msoperations.getCorrData(cutData, 0) + msoperations.getCorrData(cutData, 3)
            elif operation == 2:
                label = 'XY.YX*'
                cData = msoperations.getCorrData(cutData, 1) + msoperations.getCorrData(cutData, 2)
            num = cData.count()
            nummasked = numpy.ma.count_masked(cData)

            addInfo(pTable, ant1,ant2,ant1Name,ant2Name,statsresults, num, nummasked , label, freq, acc)
        else:
            
            intData = msoperations.getAvgData(cutData, prefaxis)
            for j in corrs:
                # For each correlation
                # From the integrated array we get mean and std of the desired complex component of each correlation
                statsresults = msoperations.getStats(msoperations.getCorrData(intData, j), compCoordDict, statparams, doUnwrap)               
                # To know the num and nummasked we have to use the cut data (before the complex component selection and axis integration)
                polCutData = msoperations.getCorrData(cutData, j)
                num = polCutData.count()
                nummasked = numpy.ma.count_masked(polCutData)
                
                addInfo(pTable, ant1,ant2,ant1Name,ant2Name, statsresults, num, nummasked, polLabels[j], freq, acc)
    outputfile.write(pTable.get_string() + '\n')
    outputfile.close()

# Add information, i.e. the label inte plot and the statistics if required
def addInfo(pTable, ant1,ant2,ant1Name,ant2Name, stats, num, nummasked, label, freq, acc):
    form = '%.'+str(acc) + 'e'
    row = [("%.3f" % freq),('%d'%ant1),('%d'%ant2),ant1Name,ant2Name,label,('%d'%num),('%d'%nummasked)]
    for i in range(len(stats)):
        row.append(form % stats[i])
    pTable.add_row(row)
    
class ASCIIStats(LModule):
    def __init__(self,userName = None):
        options = LModuleOptions()
        options.add('inputarg', 'i', 'MS path/Input RefFile/GDS file')
        options.add('output', 'r', 'Output stats folder')
        options.add('overwrite', 'd', 'Overwrite files', helpmessage =' in output folder if already there?', default = False)
        options.add('stats', 'y', 'Stats to output', helpmessage ='. The format for each stat is [complex coordinate]_[statistic]. The options for complex coordinate: amp|phase|real|imag|phaserate and for statistic: mean|median|std. Multiple values are possible', default = 'amp_mean,amp_median,amp_std,phase_mean,phase_median,phase_std,real_mean,real_median,real_std,imag_mean,imag_median,imag_std')
        options.add('column', 'c', 'Column to use', helpmessage ='. The options are DATA|CORRECTED_DATA|MODEL_DATA but also other columns that the user may have created. It is also possible to specify a combination of two columns. For example the user may want the CORRECTED_DATA-MODEL_DATA. In such example the user should write: CORRECTED_DATA,-,MODEL_DATA. Currently + and - are supported.', default = 'DATA',)
        options.add('timeslots', 't', 'Timeslots to use', helpmessage =' (comma separated and zero-based: start,end[inclusive]). Negative values work like python slicing, but please note that the second index here is inclusive. If using channel or frequency on x-axis, this parameter sets the time averaging interval.', default = '0,-1')
        options.add('channels', 's','Channels to use',  helpmessage = ' (comma separated and zero-based: start,end[inclusive]). Negative values work like python slicing, but please note that the second index here is inclusive. If using time on x-axis, this parameter sets the channel averaging interval.', default =  '0,-1')
        options.add('antennas', 'e', 'Antennas to use', helpmessage =' (comma separated list, zero-based). To specify an inclusive range of antennas use .. format, e.g. -e 0..9 requests the first 10 antennas. To see which antennas are available use uvplot -q with some of the ms. Use -1 to select all the antennas.', default = '-1')
        options.add('baselines','b', 'Baselines to use', helpmessage =' (comma separated list, zero-based), specify baselines as [st1]-[st2], if this option is used the antennas and autocorr options will be ignored', mandatory = False)
        options.add('correlations',  'p', 'Correlations to use', helpmessage =' (it does not convert, so use integers as in the MS)', default = '0,1,2,3')
        options.add('wrap', 'w', 'Unwrap phase?', default = False)
        options.add('flag', 'f', 'Show flagged data?', default = False)
        options.add('colflag', 'g', 'Column that contains flags', default = 'FLAG')
        options.add('stokes', 'k', 'Convert to Stokes IQUV?', default = False)
        options.add('autocorr', 'u', 'Show autocorrelations?', helpmessage =', this refers to the baseline correlations, nor the polarization correlations', default = False)
        options.add('operation', 'o', 'Special operation', helpmessage =' to be used over the correlations. 0 is for none operation (normal correlations are used), 1 is XX-YY and 2 is XY.YX*. If some operation is specified the options correlations and stokes are ignored.', default = '0',  choice = ['0','1','2'])
        options.add('acc', 'a', 'Accuracy',helpmessage = ' in the given statistics', default = 4)
        options.add('numprocessors', 'j', 'Simultaneous processes', default = 1)
        options.add('numnodes', 'n', 'Simultaneous nodes', default = 64)
        options.add('initfile', 'l', 'Init file',helpmessage = ', this file is "sourced" in each remote node before execution', default = lconfig.INIT_FILE)
        
        # the information
        information = 'Writes in an ASCII file statistics of the data.'
        
        # Initialize the parent class
        LModule.__init__(self, options, information, )
        
    # Function used for the tasksdistributor
    def function(self, identifier, what, childIndex, grandChildIndex, taskIndex):
        node = identifier
        absPath = what
        currentNode = utils.getHostName()
        command = 'source ' + self.initfile + ' ; python -c "import ' + __name__ + '; ' + __name__ + '.' + processMS.__name__ + '(\\\"' + absPath +'\\\",\\\"' + str(self.output) +'\\\",\\\"' + str(self.overwrite) +'\\\",\\\"' + str(self.stats) +'\\\",\\\"' + str(self.column) +'\\\",\\\"' + str(self.timeslots)  +'\\\",\\\"' + str(self.channels)  +'\\\",\\\"' + str(self.antennas)  +'\\\",\\\"'  + str(self.baselines)  +'\\\",\\\"'  + str(self.correlations)  +'\\\",\\\"' + str(self.wrap)  +'\\\",\\\"' + str(self.flag)   +'\\\",\\\"' + str(self.colflag)   +'\\\",\\\"' + str(self.stokes)   +'\\\",\\\"' + str(self.autocorr)   +'\\\",\\\"' + str(self.operation) +'\\\",\\\"' + str(self.acc) +'\\\")"'
        if node != currentNode:
            command =  'ssh ' + node + " '" + command + "'"
        (out, err) = td.execute(command)     
        if err != '':
            raise Exception(err)    
    
    def process(self, inputarg,output,overwrite, stats,column,timeslots,channels,antennas,baselines,correlations, wrap, flag, colflag, stokes, autocorr,operation,acc,numprocessors,numnodes,initfile):
        inputarg = os.path.abspath(inputarg)
        self.initfile = os.path.abspath(initfile)
        output = utils.formatPath(output)
        os.system('mkdir -p ' + output)
        self.output = os.path.abspath(output)
        self.acc = acc
        self.stats = stats
        self.column = column
        self.timeslots = timeslots
        self.channels = channels
        self.antennas =antennas
        self.baselines=baselines
        self.correlations = correlations
        self.wrap = wrap
        self.flag = flag
        self.colflag =colflag
        self.stokes = stokes
        self.autocorr = autocorr
        self.operation = operation
        self.overwrite = overwrite
        self.messages = []        
        
        if inputarg.count(ReferenceFile.EXTENSION) > 0:
            refFile = ReferenceFile(inputarg)
            (absPaths,nodes) = (refFile.absPaths,refFile.nodes)
        elif inputarg.lower().endswith('gds'):
            (absPaths,nodes) = utils.gdsToPathNode(inputarg)
        else:
            # We assume single MS
            (absPaths,nodes) = ([inputarg,],[utils.getHostName(),])
        
        if not len(absPaths):
            print "No MSs to be processed!"
            return
        
        print 'Collecting in the nodes...'
        # Run it
        retValuesKo = td.distribute(nodes, absPaths, self.function, numprocessors, numnodes)[1]
        if len(retValuesKo):
            print 'Errors:'
            for retValueKo in retValuesKo:
                print retValueKo
                
        globaloutput = self.output + '/GLOBAL_STATS'
        if os.path.isfile(globaloutput):
            os.system('rm ' + globaloutput)
        ndir = len(os.listdir(self.output))
        if ndir > 1:
            print 'Collecting finished. Joining results in ' + globaloutput
            os.system('cat ' + self.output + '/* > ' + globaloutput)
        elif ndir == 1:
            print 'Collecting finished. Check results in ' + self.output
        else:
            print 'None statistic files have been generated'
            
