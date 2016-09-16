#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import os, optparse, sys

#Generate the copy commands (using ExecuteLModule CopyData) to copy data from Target to EoR

opt = optparse.OptionParser()
opt.add_option('-r','--reffiles',help='RefFiles (if multiple, specify them comma-separated). The reffiles names must have the following format: LXXXXX[_VVV][_BY]_TARGET.ref where [VVV] is optional and is for the version (if not specified we assume version 0) and BY is also optional and is for the beam',default='')
opt.add_option('-n','--nodes',help='Nodes',default='')
options, arguments = opt.parse_args()

if options.nodes == '':
    print 'ERROR: specify the nodes'
    sys.exit(1)

if options.reffiles == '':
    print 'ERROR: specify some reffile'
    sys.exit(1)
    
oreffiles = options.reffiles.split(',')
for i in range(len(oreffiles)):
    basename = os.path.basename(oreffiles[i]).replace('.ref','').replace('_TARGET','')
    bfields = basename.split('_')
    observation = None
    beam = ''
    version = 0
    for bfield in bfields:
        if bfield.count('L'):
            observation = bfield.replace('L','')
        elif bfield.count('B'):
            beam = '_' + bfield
        else:
            version = int(bfield)
    if version > 0:
        opath =  '/data3/users/lofareor/pipeline/L' + observation + ('_%03d' % version)
    else:
        opath =  '/data3/users/lofareor/L' + observation
    print 'ExecuteLModule CopyData -i ' + oreffiles[i] + ' -o ' + basename + '.ref -c ' + opath + ' -u ' + options.nodes + ' -n 32 -s logs' + basename
