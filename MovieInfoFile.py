#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
from ledama import utils

class MovieInfoFile:
    def __init__(self, filepath, useddata = None, xaxis = None, jones = None, stations = None, refstation = None,  polar = None, times = None, channels = None, yrange = None, delays = None, message= None):
        self.filepath = filepath
        (self.xaxis, self.jones, self.polar, self.refstation, self.times, self.channels, self.yrange, self.message) = (xaxis, jones, polar, refstation, times, channels, yrange, message)
        (self.useddata, self.stations, self.delays) = (useddata, stations, delays)
            
    def write(self, useddata = None, xaxis = None, jones = None, stations = None, refstation = None,  polar = None, times = None, channels = None, yrange = None, delays = None, message= None):
        if useddata == None: useddata = self.useddata
        if xaxis == None: xaxis = self.xaxis
        if jones == None: jones = self.jones
        if stations == None: stations = self.stations
        if refstation == None: refstation = self.refstation
        if polar == None: polar = self.polar
        if times == None: times = self.times
        if channels == None: channels = self.channels
        if yrange == None: yrange = self.yrange
        if delays == None: delays = self.delays
        if message == None: message = self.message
        infoFile = open(self.filepath, 'w')
        infoFile.write('XAXIS: ' + str(xaxis) + '\n')
        infoFile.write('JONES: ' + str(jones) + '\n')
        infoFile.write('STATIONS: ' + ','.join(stations) + '\n')
        ds = []
        for station in stations:
            ds.append(str(delays[station]))
        infoFile.write('DELAYS: ' + ','.join(ds) + '\n')
        infoFile.write('REFSTATION: ' + str(refstation) + '\n')
        infoFile.write('POLAR: ' + str(polar) + '\n')
        infoFile.write('TIMES: ' + str(times) + '\n')
        infoFile.write('CHANNELS: ' + str(channels) + '\n')
        infoFile.write('YRANGE: ' + str(yrange) + '\n')
        infoFile.write('MESSAGE: ' + str(message) + '\n')
        infoFile.write('USEDDATA:\n')
        mslines = []
        for msData in useddata:
            if len(msData) == 3:
                (msId, freq, sbIndex) = msData
                mslines.append(('%03d' % sbIndex) + ' ' + str(freq) + ' ' + str(msId) + '\n')
            elif len(msData) == 2:
                (node,msPath) = msData
                infoFile.write(node + ' ' + msPath + '\n')
        for msline in sorted(mslines):
            infoFile.write(msline)
        infoFile.close()

    def read(self, filepath = None):
        if filepath == None:
            filepath = self.filepath
        # Open the file
        infoLines = open(filepath,  "r").read().split('\n')
        self.useddata = []
        usedDataIndex = -1
        for lineIndex in range(len(infoLines)):
            line = infoLines[lineIndex]
            if line.count('XAXIS:'):
                self.xaxis = line.replace('XAXIS:','').strip()
            elif line.count('JONES:'):
                self.jones = line.replace('JONES:','').replace('[','').replace(']','').strip().split(',')
                for i in range(len(self.jones)): self.jones[i] = int(self.jones[i])
            elif line.count('POLAR:'):
                self.polar = utils.booleanStringToBoolean(line.replace('POLAR:','').strip())
            elif line.count('STATIONS:'):
                self.stations = line.replace('STATIONS:','').strip().split(',')
            elif line.count('DELAYS:'):
                tempdelays = line.replace('DELAYS:','').strip().split(',')
                self.delays = {}
                for i in range(len(tempdelays)): self.delays[self.stations[i]] = float(tempdelays[i])
            elif line.count('REFSTATION:'):
                self.refstation = line.replace('REFSTATION:','').strip()
                if self.refstation == '':
                    self.refstation = None
            elif line.count('TIMES:'):
                self.times = line.replace('TIMES:','').replace('[','').replace(']','').strip().split(',')
                for i in range(len(self.times)): self.times[i] = int(self.times[i])
            elif line.count('CHANNELS:'):
                self.channels = line.replace('CHANNELS:','').replace('[','').replace(']','').strip().split(',')
                for i in range(len(self.channels)): self.channels[i] = int(self.channels[i])
            elif line.count('YRANGE:'):
                self.yrange = line.replace('YRANGE:','').replace('[','').replace(']','').strip().split(',')
                for i in range(len(self.yrange)): self.yrange[i] = float(self.yrange[i])
            elif line.count('MESSAGE:'):
                self.message = line.replace('MESSAGE:','').strip()
                if self.message == '':
                    self.message = None
            elif line.count('USEDDATA:'):
                usedDataIndex = lineIndex
                break
        if usedDataIndex < 0:
            errormessage = 'ERROR: No used data detected!'
            raise Exception(errormessage)
        
        for lineIndex in range(usedDataIndex+1,len(infoLines)):
            fields = infoLines[lineIndex].split(' ')
            if len(fields) == 3:
                (sbIndex, freq, msId) = fields
                self.useddata.append((int(msId), float(freq), int(sbIndex)))
            elif len(fields) == 2:
                self.useddata.append(fields)