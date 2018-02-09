
#ifndef PAGE_REC_ITER_C
#define PAGE_REC_ITER_C

using namespace std;

#include "../headers/MyDB_PageRecIterator.h"

void MyDB_PageRecIterator::getNext() {
    this->myRecPtr->fromBinary((char*)this->myPage->getBytes() + this->offset);// pos = original + offset
    this->offset += this->myRecPtr->getBinarySize();
}

bool MyDB_PageRecIterator::hasNext() {
//    maybe need to refer to: https://piazza.com/class/jc6ed4h5nkg4tz?cid=73
}

MyDB_PageRecIterator::MyDB_PageRecIterator (MyDB_PageReaderWriter &myParent, MyDB_PageHandle myPage, MyDB_RecordPtr myRecPtr): myParent(myParent), myPage(myPage), myRecPtr(myRecPtr){
    this->offset = sizeof(MyDB_PageType) + sizeof(size_t);// header = pageType + offset
}

#endif