#ifndef PAGE_C
#define PAGE_C

//#include "MyDB_BufferManager.h"
//#include "MyDB_Page.h"
//#include "MyDB_Table.h"

#include "../headers/MyDB_BufferManager.h"
#include "../headers/MyDB_Page.h"
#include "../../Catalog/headers/MyDB_Table.h"

MyDB_Page :: MyDB_Page(MyDB_TablePtr whichTable, long page_id, bool pinned, MyDB_BufferManager& bufferManager, long timeStamp, bool buffered)
        :whichTable(whichTable), page_id(page_id), pinned(pinned), bufferManager(bufferManager), timeStamp(timeStamp), buffered(buffered){
	this->dirty = false;
	this->refCount = 0;
}

MyDB_Page :: ~MyDB_Page() {

}

void* MyDB_Page :: getBytes() {
    bufferManager.process(*this);
	return bytes;
}

void MyDB_Page :: removeRef() {
	this->refCount -= 1;
	if (this->refCount == 0) {
        bufferManager.remove(*this);
	}
}

#endif