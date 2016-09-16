#!/usr/bin/env python
################################################################################
#    Created by Oscar Martinez                                                 #
#    martinez@astro.rug.nl                                                     #
################################################################################
import optparse, sys

DEFAULT_NODES = '43-74'
DEFAULT_DISKS = '3,1,2,1'
DEFAULT_TEMPLATE_1 = '/home/users/lofardata/NDPPP/NDPPP_AOflag_avg64to15.parset'
DEFAULT_TEMPLATE_2 = '/home/users/lofardata/NDPPP/NDPPP_avg15to3.parset'
DEFAULT_TEMPLATE_3 = '/home/users/lofardata/NDPPP/NDPPP_avg3to1.parset'
DEFAULT_NDPPP_SOURCE = '/home/users/lofardata/NDPPP/initndppp.sh'

opt = optparse.OptionParser()
opt.add_option('-o','--observation',help='Observation (L12345 for example)',default='')
opt.add_option('-n','--nodes',help='Nodes, provide only the indexes. For example 11-42 or 43-74 (default is ' + DEFAULT_NODES + ')',default=DEFAULT_NODES)
opt.add_option('-d','--disks',help='Disks to use for the different versions. The format must be [raw],[15],[3],[1]. For example, 3,1,2,1 would look for the raw data in data3 disks. Then it would create the 15ch data in data1, the 3ch in data2 and the 1ch in data1 (default is ' + DEFAULT_DISKS + ')',default=DEFAULT_DISKS)
opt.add_option('-a','--template1',help='Template NDPPP parset file to be used for the version 1, usually NDPPP 64->15 (default is ' + DEFAULT_TEMPLATE_1 + ')',default=DEFAULT_TEMPLATE_1)
opt.add_option('-b','--template2',help='Template NDPPP parset file to be used for the version 2, usually NDPPP 15->3 (default is ' + DEFAULT_TEMPLATE_2 + ')',default=DEFAULT_TEMPLATE_2)
opt.add_option('-c','--template3',help='Template NDPPP parset file to be used for the version 3, usually NDPPP 3->1 (default is ' + DEFAULT_TEMPLATE_3 + ')',default=DEFAULT_TEMPLATE_3)
opt.add_option('-s','--ndpppsource',help='NDPPP source file. This file is sourced before each NDPPP execution. It can be used to set number of used cores  (default is ' + DEFAULT_NDPPP_SOURCE + ')',default=DEFAULT_NDPPP_SOURCE)
options, arguments = opt.parse_args()

if options.observation == '':
    print 'ERROR: specify the observation'
    sys.exit(1)
observation = options.observation.replace('L','')

disks = options.disks.split(',')
if len(disks) != 4:
    print 'ERROR: specify the 4 disks'
    sys.exit(1)
for disk in disks:
    if int(disk) not in (1,2,3):
        print 'ERROR: disk values are 1,2 or 3'
        sys.exit(1)    

print 'ExecuteLModule CreateRefFileFromPath -i /data' + disks[0] + '/users/lofareor/L2013_' + observation + ' -o L' + observation + '_000_raw.ref -s ' + options.nodes

print 'ExecuteLModule CreateNDPPPParsetFiles -i L' + observation + '_000_raw.ref -t ' + options.template1 + ' -o parsets_NDPPP_001/ -c /data' + disks[1] + '/users/lofareor/pipeline/L2013_' + observation + '_001'
print 'ExecuteLModule LaunchNDPPP -i parsets_NDPPP_001/ -n 32 -p 1 -l logs_NDPPP_001' + ' -s ' + options.ndpppsource
print 'ExecuteLModule CreateRefFileFromPath -i /data' + disks[1] + '/users/lofareor/pipeline/L2013_' + observation + '_001/ -o L' + observation + '_001_avg15ch.ref -s ' + options.nodes

print 'ExecuteLModule CreateNDPPPParsetFiles -i L' + observation + '_001_avg15ch.ref -t ' + options.template2 + ' -o parsets_NDPPP_002/ -c /data' + disks[2] + '/users/lofareor/pipeline/L2013_' + observation + '_002'
print 'ExecuteLModule LaunchNDPPP -i parsets_NDPPP_002/ -n 32 -p 1 -l logs_NDPPP_002' + ' -s ' + options.ndpppsource
print 'ExecuteLModule CreateRefFileFromPath -i /data' + disks[2] + '/users/lofareor/pipeline/L2013_' + observation + '_002/ -o L' + observation + '_002_avg3ch.ref -s ' + options.nodes

print 'ExecuteLModule CreateNDPPPParsetFiles -i L' + observation + '_002_avg3ch.ref -t ' + options.template3 + ' -o parsets_NDPPP_003/ -c /data' + disks[3] + '/users/lofareor/pipeline/L2013_' + observation + '_003'
print 'ExecuteLModule LaunchNDPPP -i parsets_NDPPP_003/ -n 32 -p 1 -l logs_NDPPP_003' + ' -s ' + options.ndpppsource
print 'ExecuteLModule CreateRefFileFromPath -i /data' + disks[3] + '/users/lofareor/pipeline/L2013_' + observation + '_003/ -o L' + observation + '_003_avg1ch.ref -s ' + options.nodes