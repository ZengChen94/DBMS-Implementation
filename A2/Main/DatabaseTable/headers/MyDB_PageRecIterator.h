
#ifndef PAGE_REC_ITER_H
#define PAGE_REC_ITER_H

#include <memory>

using namespace std;

#include "../../BufferMgr/headers/MyDB_Page.h"
#include "../../BufferMgr/headers/MyDB_PageHandle.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_RecordIterator.h"
#include "../../Record/headers/MyDB_Record.h"

class MyDB_PageRecIterator;

typedef shared_ptr<MyDB_PageRecIterator> MyDB_PageRecIteratorPtr;

class MyDB_PageRecIterator : public MyDB_RecordIterator {

public:

    // put the contents of the next record in the file/page into the iterator record
    // this should be called BEFORE the iterator record is first examined
    void getNext();

    // return true iff there is another record in the file/page
    bool hasNext();

    // destructor and contructor
    MyDB_PageRecIterator(MyDB_PageHandle myPage, MyDB_RecordPtr myRecPtr);

    ~MyDB_PageRecIterator() {};

private:
//    MyDB_PageReaderWriter &myParent;
    MyDB_PageHandle myPage;
    MyDB_RecordPtr myRecPtr;

    size_t offset;
};

#endif