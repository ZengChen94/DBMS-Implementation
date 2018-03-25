## COMP 530: Database System Implementation

####Introduction

This repo is for assignments of 17Fall COMP 530: Database System Implementation. The whole work of these assignments are about database management system architecture, buffer management, query processing and optimization, transaction processing, concurrency control and recovery, data storage, indexing structures, and related topics. They're all about C++ and DBMS.

####Authors

* Chen Zeng (cz39)
* Yuanqing Zhu (yz120)

####Contents

* A0 C++ Warm-Up: out Monday
* A1 Buffer and file management
* A3 Sorted file implementation
* A4 B+-tree implementation
* A5 SQL type checking
* A6 Relational operators
* A7 Putting it all together
* A8 Multi-threading

####Usage

To get any credit on these assignments, the code must be compiled and run on Clear. 

However, clear does not have SCons installed. So in the home directory, follow the following steps:

1. Download SCons: `wget --no-check-certificate https://pypi.python.org/`
2. Unpack it: `gunzip scons-2.4.1.tar.gz` `tar xvf scons-2.4.1.tar`
3. Build it: `mkdir scons` `cd scons-2.4.1/` `python setup.py install --prefix=../scons` `cd ..` `rm -r scons-2.4.1/`
4. Run it: `~/scons/bin/scons-2.4.1`

As for testing the projects, in the directory of project, follow the following steps:

1. Enter directory ./Build: `cd ./Build/`
2. Compile and Build it: `~/scons/bin/scons-2.4.1`
3. Select the module(s) you want to build or clean.
4. Test it: `./bin/stackUnitTest`