## COMP 530: Database System Implementation

To get any credit on these assignments, the code must be compiled and run on Clear. 

However, clear does not have SCons installed. So in the home directory, follow the following steps:

1. Download SCons: `wget --no-check-certificate https://pypi.python.org/`
2. Unpack it: `gunzip scons-2.4.1.tar.gz` `tar xvf scons-2.4.1.tar`
3. Build it: `mkdir scons` `[cmj4@glass ~]$` `python setup.py install --prefix=../scons` `cd ..` `rm -r scons-2.4.1/`
4. Run it: `~/scons/bin/scons-2.4.1`

As for testing the projects, in the directory of project, follow the following steps:

1. Enter directory ./Build: `cd ./Build/`
2. Compile and Build it: `~/scons/bin/scons-2.4.1`
3. Select the module(s) you want to build or clean.
4. Test it: `./bin/stackUnitTest`

Authors:

* Chen Zeng (cz39)
* Yuanqing Zhu (yz120)