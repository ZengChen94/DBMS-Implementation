
#ifndef PAGE_RW_H
#define PAGE_RW_H

//#include "MyDB_PageType.h"
#include "MyDB_TableReaderWriter.h"

#include "../../Catalog/headers/MyDB_PageType.h"

struct PageOverlay {
    unsigned offsetToNextUnwritten;
    /* other meta data here */
    MyDB_PageType pageType;
    char bytes[0]; /* this is where the data will be */
};

class MyDB_PageReaderWriter {

public:

    // ANY OTHER METHODS YOU WANT HERE

    // empties out the contents of this page, so that it has no records in it
    // the type of the page is set to MyDB_PageType :: RegularPage
    void clear();

    // return an itrator over this page... each time returnVal->next () is
    // called, the resulting record will be placed into the record pointed to
    // by iterateIntoMe
    MyDB_RecordIteratorPtr getIterator(MyDB_RecordPtr iterateIntoMe);

    // appends a record to this page... return false is the append fails because
    // there is not enough space on the page; otherwise, return true
    bool append(MyDB_RecordPtr appendMe);

    // gets the type of this page... this is just a value from an ennumeration
    // that is stored within the page
    MyDB_PageType getType();

    // sets the type of the page
    void setType(MyDB_PageType toMe);

    MyDB_PageReaderWriter(MyDB_TablePtr myTable, MyDB_BufferManagerPtr myBuffer, int whichPage);

private:

    // ANYTHING ELSE YOU WANT HERE
    MyDB_PageHandle myPage;
    MyDB_BufferManagerPtr myBuffer;
};

#endif