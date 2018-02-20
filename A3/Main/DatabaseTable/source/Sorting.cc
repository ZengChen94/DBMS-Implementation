
#ifndef SORT_C
#define SORT_C

//#include "../headers/RecordComparator.h"
#include "../headers/MyDB_PageReaderWriter.h"
#include "../headers/MyDB_TableRecIterator.h"
#include "../headers/MyDB_TableRecIteratorAlt.h"
#include "../headers/MyDB_TableReaderWriter.h"
#include "../headers/Sorting.h"

using namespace std;

class RecordComparator {

public:

    RecordComparator (function <bool ()> comparatorIn, MyDB_RecordPtr lhsIn, MyDB_RecordPtr rhsIn) {
        comparator = comparatorIn;
        lhs = lhsIn;
        rhs = rhsIn;
    }

    bool operator() (const MyDB_RecordIteratorAltPtr leftIter, const MyDB_RecordIteratorAltPtr rightIter) const {
        leftIter->getCurrent (lhs);
        rightIter->getCurrent (rhs);
        return !comparator ();
    }

private:
    function <bool ()> comparator;
    MyDB_RecordPtr lhs;
    MyDB_RecordPtr rhs;
};

void append (MyDB_BufferManagerPtr parent, MyDB_PageReaderWriter &curPage, vector <MyDB_PageReaderWriter> &pageReaderWriter, MyDB_RecordPtr appendMe)
{
    bool flag = curPage.append(appendMe);
    if (flag == false) {
        pageReaderWriter.push_back(curPage);
        MyDB_PageHandle pageHandle = parent->getPage();
        MyDB_PageReaderWriter nextPage(*parent);
        nextPage.append(appendMe);
        curPage = nextPage;
    }
}

vector <MyDB_PageReaderWriter> mergeSort (MyDB_BufferManagerPtr bufferMgr, vector <MyDB_PageReaderWriter> &pages, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs, int low, int high) {
    if (low == high) {
        vector <MyDB_PageReaderWriter> result;
        result.push_back(pages[low]);
        return result;
    }
    else {
        int mid = (low + high) / 2;
        vector <MyDB_PageReaderWriter> pages1 = mergeSort(bufferMgr, pages, comparator, lhs, rhs, low, mid);
        vector <MyDB_PageReaderWriter> pages2 = mergeSort(bufferMgr, pages, comparator, lhs, rhs, mid + 1, high);
        MyDB_RecordIteratorAltPtr leftIter = getIteratorAlt(pages1);
        MyDB_RecordIteratorAltPtr rightIter = getIteratorAlt(pages2);
        return mergeIntoList(bufferMgr, leftIter, rightIter, comparator, lhs, rhs);
    }
}

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    // how to use comparator?
    RecordComparator recordComparator (comparator, lhs, rhs);
    priority_queue <MyDB_RecordIteratorAltPtr, vector <MyDB_RecordIteratorAltPtr>, RecordComparator> priorityQueue(recordComparator);

    for (MyDB_RecordIteratorAltPtr recordPtr : mergeUs) {
        if (recordPtr->advance()) {
            priorityQueue.push(recordPtr);
        }
    }

    while (priorityQueue.size() != 0) {
        MyDB_RecordIteratorAltPtr recordPtr = priorityQueue.top();
        priorityQueue.pop();

        recordPtr->getCurrent(lhs);
        sortIntoMe.append(lhs);

        if (recordPtr->advance()) {
            priorityQueue.push(recordPtr);
        }
    }

}

vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter, MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    vector <MyDB_PageReaderWriter> result;
    MyDB_PageHandle pageHandle = parent->getPage();
    MyDB_PageReaderWriter curPage(*parent);
    int flag = 0;

    while (true) {
        leftIter->getCurrent(lhs);
        rightIter->getCurrent(rhs);
        RecordComparator recordComparator(comparator, lhs, rhs);
        if (comparator()) {
            append(parent, curPage, result, lhs);
            if (!leftIter->advance()) {
                append(parent, curPage, result, rhs);
                flag = 1;
                break;
            }
        } else {
            append(parent, curPage, result, rhs);
            if (!rightIter->advance()) {
                append(parent, curPage, result, lhs);
                flag = -1;
                break;
            }
        }
    }

    if (flag == 1) {
        while (rightIter->advance()) {
            rightIter->getCurrent (rhs);
            append(parent, curPage, result, rhs);
        }
    }
    else if (flag == -1) {
        while (leftIter->advance()) {
            leftIter->getCurrent (lhs);
            append(parent, curPage, result, lhs);
        }
    }

    result.push_back(curPage);
    return result;
}
	
void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
    int numPages = sortMe.getNumPages();
    int cntPages = 0;
    vector <vector <MyDB_PageReaderWriter>> unsortedRunList;
    vector <MyDB_RecordIteratorAltPtr> recordIters;

    // collect runs
    while (cntPages < numPages) {
        vector <MyDB_PageReaderWriter> unsortedRun;
        for (int i = 0; i < runSize; i++) {
            if (cntPages < numPages)
                unsortedRun.push_back(*(sortMe[cntPages++].sort(comparator, lhs, rhs)));
            else
                break;
        }
        unsortedRunList.push_back(unsortedRun);
    }

    // sort runs
    for (auto unsortedRun : unsortedRunList) {
        vector <MyDB_PageReaderWriter> sortedRun = mergeSort(sortMe.getBufferMgr(), unsortedRun, comparator, lhs, rhs, 0, unsortedRun.size()-1);
        recordIters.push_back(getIteratorAlt(sortedRun));
    }

    // merge runs
    mergeIntoFile (sortIntoMe, recordIters, comparator, lhs, rhs);
}

#endif
