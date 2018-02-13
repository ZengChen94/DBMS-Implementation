
#ifndef PAGE_RW_C
#define PAGE_RW_C

//#include "MyDB_PageReaderWriter.h"

#include "../../DatabaseTable/headers/MyDB_PageReaderWriter.h"
#include "../../DatabaseTable/headers/MyDB_PageRecIterator.h"

void MyDB_PageReaderWriter::clear() {
//    *((size_t *) ((char *) this->myPage->getBytes() + sizeof(MyDB_PageType))) =
//            sizeof(MyDB_PageType) + sizeof(size_t);// initialize the offset size
//    *((MyDB_PageType *) this->myPage->getBytes()) = MyDB_PageType::RegularPage;// initialize the pageType
//    this->myPage->wroteBytes();
//
    PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
    myPageOverlay->offsetToNextUnwritten = 0;
    this->setType(MyDB_PageType::RegularPage);
    this->myPage->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter::getType() {
//    return *((MyDB_PageType *) this->myPage->getBytes());

    PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
    return myPageOverlay->pageType;
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter::getIterator(MyDB_RecordPtr iterateIntoMe) {
    return make_shared<MyDB_PageRecIterator>(this->myPage, iterateIntoMe);
}

void MyDB_PageReaderWriter::setType(MyDB_PageType toMe) {
//    *((MyDB_PageType *) this->myPage->getBytes()) = toMe;

    PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
    myPageOverlay->pageType = toMe;
    this->myPage->wroteBytes();
}

bool MyDB_PageReaderWriter::append(MyDB_RecordPtr appendMe) {
//    size_t recSize = appendMe->getBinarySize();
//    char *ptr = (char *) this->myPage->getBytes();
//
//    if (*((size_t *) (ptr + sizeof(MyDB_PageType))) + recSize >
//        this->myBuffer->getPageSize())// page - offset = rest space
//        return false;
//    appendMe->toBinary(ptr + *((size_t *) (ptr + sizeof(MyDB_PageType))));
//    *((size_t *) (ptr + sizeof(MyDB_PageType))) += recSize;// update offset
//    this->myPage->wroteBytes();
//    return true;

    PageOverlay *myPageOverlay = (PageOverlay *) this->myPage->getBytes();
    size_t recSize = appendMe->getBinarySize();
    size_t pageSize = myBuffer->getPageSize();
    size_t remain = pageSize - (myPageOverlay->offsetToNextUnwritten + sizeof(PageOverlay));
    if (remain < recSize)
        return false;
    void *next = appendMe->toBinary(&(myPageOverlay->bytes[(myPageOverlay->offsetToNextUnwritten)]));
    /* and update the next available slot */
    myPageOverlay->offsetToNextUnwritten +=
            (char *) next - &(myPageOverlay->bytes[myPageOverlay->offsetToNextUnwritten]);
    this->myPage->wroteBytes();
    return true;
}

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_TablePtr myTable, MyDB_BufferManagerPtr myBuffer, int whichPage)
        : myBuffer(myBuffer) {
    this->myPage = myBuffer->getPage(myTable, whichPage);
}

#endif