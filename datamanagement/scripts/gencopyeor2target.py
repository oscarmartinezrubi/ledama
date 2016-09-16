#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import optparse, sys

#Generate the copy commands (using ExecuteLModule CopyData) to copy data from EoR to Target

opt = optparse.OptionParser()
opt.add_option('-o','--observation',help='Observation (L12345 for example)',default='')
opt.add_option('-a','--version1',help='RefFile of the version 1, usually the 15 ch data',default='')
opt.add_option('-b','--version2',help='RefFile of the version 2, usually the  3 ch data',default='')
opt.add_option('-c','--version3',help='RefFile of the version 3, usually the  1 ch data',default='')
opt.add_option('-t','--target',help='Target TIER (choose from e|f) [default e]',default='e',type='choice',choices=['e','f'])
options, arguments = opt.parse_args()

if options.observation == '':
    print 'ERROR: specify the observation'
    sys.exit(1)
observation = options.observation.replace('L','')

if (options.version15ch == '') and (options.version3ch == '') and (options.version1ch == ''):
    print 'ERROR: specify at least one RefFile'
    sys.exit(1)

if (options.version1 != ''):
    print 'ExecuteLModule CopyData -i ' + options.version1 + ' -c L' + observation + '_001 -o L' + observation + '_001_TARGET.ref -u TARGET_' + options.target.upper() + '_EOR -p 1 -n 32 -s logsL' + observation + '_001'
if (options.version2 != ''):
    print 'ExecuteLModule CopyData -i ' + options.version2 + ' -c L' + observation + '_002 -o L' + observation + '_002_TARGET.ref -u TARGET_' + options.target.upper() + '_EOR -p 1 -n 32 -s logsL' + observation + '_002'
if (options.version3 != ''):
    print 'ExecuteLModule CopyData -i ' + options.version3 + ' -c L' + observation + '_003 -o L' + observation + '_003_TARGET.ref -u TARGET_' + options.target.upper() + '_EOR -p 1 -n 32 -s logsL' + observation + '_003'    

