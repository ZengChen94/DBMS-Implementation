
#ifndef TABLE_REC_ITER_H
#define TABLE_REC_ITER_H

#include <memory>

using namespace std;

#include "../../Catalog/headers/MyDB_Table.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_RecordIterator.h"
#include "../../Record/headers/MyDB_Record.h"

class MyDB_TableRecIterator;

typedef shared_ptr<MyDB_TableRecIterator> MyDB_TableRecIteratorPtr;

class MyDB_TableRecIterator : public MyDB_RecordIterator {

public:

    // put the contents of the next record in the file/page into the iterator record
    // this should be called BEFORE the iterator record is first examined
    void getNext();

    // return true iff there is another record in the file/page
    bool hasNext();

    // destructor and contructor
    MyDB_TableRecIterator(MyDB_TableReaderWriter &myParent, MyDB_TablePtr myTablePtr, MyDB_RecordPtr myRecPtr);

    ~MyDB_TableRecIterator() {};

private:

    MyDB_TableReaderWriter &myParent;
    MyDB_TablePtr myTablePtr;
    MyDB_RecordPtr myRecPtr;
    MyDB_RecordIteratorPtr myPageIter;

    int page_index;// i^th page

};

#endif