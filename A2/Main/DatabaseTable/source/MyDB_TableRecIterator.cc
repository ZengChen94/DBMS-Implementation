
#ifndef TABLE_REC_ITER_C
#define TABLE_REC_ITER_C

//using namespace std;

#include "../headers/MyDB_TableRecIterator.h"
#include "../headers/MyDB_PageReaderWriter.h"

void MyDB_TableRecIterator::getNext() {
    this->myPageIter->getNext();
}

bool MyDB_TableRecIterator::hasNext() {
    if (this->myPageIter->hasNext()) {
        return true;
    }

    while (this->page_index < this->myTablePtr->lastPage()) {
        this->page_index += 1;
        this->myPageIter = this->myParent[this->page_index].getIterator(this->myRecPtr);
        if (this->myPageIter->hasNext()) {
            return true;
        }
    }

//    while (this->page_index <= this->myTablePtr->lastPage()) {
//        if (this->myPageIter->hasNext()){
//            return true;
//        }
//        this->page_index += 1;
//        this->myPageIter = this->myParent[this->page_index].getIterator(this->myRecPtr);
//    }

    return false;
}

MyDB_TableRecIterator::MyDB_TableRecIterator(MyDB_TableReaderWriter &myParent, MyDB_TablePtr myTablePtr,
                                             MyDB_RecordPtr myRecPtr) : myParent(myParent), myTablePtr(myTablePtr),
                                                                        myRecPtr(myRecPtr) {
    this->page_index = 0;
    this->myPageIter = this->myParent[this->page_index].getIterator(
            this->myRecPtr);// get the 0^th page and return the pageIterator
}

#endif