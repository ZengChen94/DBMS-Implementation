//
// Created by Chen Zeng on 4/28/18.
//


#ifndef REG_SELECTION_C
#define REG_SELECTION_C

#include "RegularSelectionMultiThread.h"

RegularSelectionMultiThread :: RegularSelectionMultiThread (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                                      string selectionPredicateIn, vector <string> projectionsIn, int threadNumIn) {
    input = inputIn;
    output = outputIn;
    selectionPredicate = selectionPredicateIn;
    projections = projectionsIn;
    threadNum = threadNumIn;
}

void RegularSelectionMultiThread :: run() {
    int pageNumber = input->getNumPages();
    int partitionSize = pageNumber / threadNum;
    vector<thread> threadList;
    for (int i = 0; i < threadNum; i++) {
        if (i != threadNum-1)
            threadList.push_back(thread(&RegularSelectionMultiThread::execThread, this, i*partitionSize, (i+1)*partitionSize-1));
        else
            threadList.push_back(thread(&RegularSelectionMultiThread::execThread, this, i*partitionSize, pageNumber-1));
    }
    for(auto& thread : threadList) {
        thread.join();
    }
}

void RegularSelectionMultiThread :: execThread(int low, int high) {
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    MyDB_RecordPtr outputRec = output->getEmptyRecord ();

    // compile all of the coputations that we need here
    vector <func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back (inputRec->compileComputation (s));
    }
    func pred = inputRec->compileComputation (selectionPredicate);

    // now, iterate through the B+-tree query results
//    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt ();
    MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt (low, high);
    while (myIter->advance ()) {

        myIter->getCurrent (inputRec);

        // see if it is accepted by the predicate
        if (!pred()->toBool ()) {
            continue;
        }

        // execute all of the computations
        int i = 0;
        for (auto &f : finalComputations) {
            outputRec->getAtt (i++)->set (f());
        }

        outputRec->recordContentHasChanged ();
        output->append (outputRec);
    }
}

#endif
