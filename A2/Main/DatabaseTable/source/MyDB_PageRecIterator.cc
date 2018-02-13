
#ifndef PAGE_REC_ITER_C
#define PAGE_REC_ITER_C

//using namespace std;

#include "../headers/MyDB_PageRecIterator.h"

void MyDB_PageRecIterator::getNext() {
//    this->myRecPtr->fromBinary((char *) this->myPage->getBytes() + this->offset);// pos = original + offset
//    this->offset += this->myRecPtr->getBinarySize();

    if (this->hasNext()) {
        PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
        this->myRecPtr->fromBinary(&(myPageOverlay->bytes[this->offset]));
        this->offset += this->myRecPtr->getBinarySize();
    }
}

bool MyDB_PageRecIterator::hasNext() {
    // reference: https://piazza.com/class/jc6ed4h5nkg4tz?cid=73
//    if (*((size_t *) (((char *) this->myPage->getBytes()) + sizeof(MyDB_PageType))) != this->offset)
//        return true;
//    else
//        return false;

    PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
    if (this->offset < myPageOverlay->offsetToNextUnwritten)
        return true;
    return false;
}

MyDB_PageRecIterator::MyDB_PageRecIterator(MyDB_PageHandle myPage, MyDB_RecordPtr myRecPtr) : myPage(myPage),
                                                                                              myRecPtr(myRecPtr) {
//    this->offset = sizeof(MyDB_PageType) + sizeof(size_t);// header = pageType + offset

    this->offset = 0;
}

#endif