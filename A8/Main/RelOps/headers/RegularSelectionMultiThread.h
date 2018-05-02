//
// Created by Chen Zeng on 4/28/18.
//

#ifndef A7_REGULARSELECTIONMULTITHREAD_H
#define A7_REGULARSELECTIONMULTITHREAD_H

#include "MyDB_TableReaderWriter.h"
#include <string>
#include <utility>
#include <vector>
#include <thread>

// this class encapsulates a simple, scan-based selection

class RegularSelectionMultiThread {

public:
    //
    // The string selectionPredicate encodes the predicate to be executed.
    //
    // The vector projections contains all of the computations that are
    // performed to create the output records (see the ScanJoin for an example).
    //
    // Record are read from input, and written to output.
    //
    RegularSelectionMultiThread (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                      string selectionPredicate, vector <string> projections, int threadNumIn);

    // execute the selection operation
    void run ();
    void execThread(int low, int high);

private:

    MyDB_TableReaderWriterPtr input;
    MyDB_TableReaderWriterPtr output;
    string selectionPredicate;
    vector <string> projections;
    int threadNum;
};

#endif //A7_REGULARSELECTIONMULTITHREAD_H
