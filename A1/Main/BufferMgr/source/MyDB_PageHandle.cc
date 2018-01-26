
#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
//#include "MyDB_PageHandle.h"
#include "../headers/MyDB_PageHandle.h"

void *MyDB_PageHandleBase :: getBytes () {
	return this->page->getBytes();
}

void MyDB_PageHandleBase :: wroteBytes () {
//    cout << "wroteBytes is called" << endl;
    this->page->wroteBytes();
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
    this->page->removeRef();
}

#endif

