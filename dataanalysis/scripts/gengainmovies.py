#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os,optparse, sys
from ledama.leddb import LEDDBOps
from ledama.leddb.Connector import Connector
from ledama.leddb.Naming import LDSBP, LDSB, LDS, ID, NAME, VERSION

# Generate the commands to create GAIN time movies

opt = optparse.OptionParser()
opt.add_option('-i','--ldsbpids',help='LDSBP Ids from which we want to generate the Gain movies. If multiple specify them comma-separated',default='')
opt.add_option('-o','--output',help='Output for the movies. Note that the movies will be in a subfolder of this directory: [output]/[LDS]/[LDS]_[version]_time/',default='')
opt.add_option('-e','--execpath',help='Execution path where the temporal files are located (this must be a shared path like home directory)',default='')
opt.add_option('-n','--nodes',help='Nodes to use, you can use for example 11-74 or 43-74, or 1,2,3,4,8-10',default='')
options, arguments = opt.parse_args()

parentPath = '/data3/users/lofareor/movies'
outputFolder = '/home/users/lofardata/martinez/animations'


for option in (options.ldsbpids, options.output, options.execpath, options.nodes):
    if options.ldsbpids == '':
        print 'ERROR: specify all the options'
        sys.exit(1)
ldsbpids = options.ldsbpids.split(',')
outputFolder = os.path.abspath(options.output)
execFolder = os.path.abspath(options.execpath)


connection = Connector().getConnection()
for ldsbpid in ldsbpids:
    (lds, version) = LEDDBOps.select(connection, [LDS,LDSB,LDSBP], {LDSBP+'.'+LDSBP+ID:ldsbpid, LDSBP+'.'+LDSB+ID : LDSB+'.'+LDSB+ID, LDSB+'.'+LDS: LDS+'.'+NAME}, [LDS+'.'+NAME, LDSBP+'.'+VERSION])[0]
    animBaseName = lds + ('_%03d' % version) + '_38SBs_time'
    loutputPath = outputFolder + '/' + lds + '/' + animBaseName
    print  'rm -rf ' + loutputPath
    print  'mkdir -p ' + loutputPath
    
    diagFilePath = loutputPath + '/' + animBaseName + '.diag'
    print 'ExecuteLModule GetGAINs -p ' + str(ldsbpid) + ' -b 0..380..10 -o ' + diagFilePath
    
    tempOut = execFolder + '/' + animBaseName
    print 'ExecuteLModule GAINAnimation -i ' + diagFilePath + ' -o ' + tempOut + ' -n ' + options.nodes + ' -r 0.0015,0.0025,-3.14,3.14'
    movieFilePath = loutputPath + '/' + animBaseName + '.mp4'
    print "ffmpeg -r 20 -i " + tempOut + "/img%06d.png -vcodec mpeg4 -b:v 4620000 -y " + movieFilePath  
    print 'cp ' + tempOut + '/INFO ' + loutputPath
    print 'rm -rf ' + tempOut
    print
connection.close()