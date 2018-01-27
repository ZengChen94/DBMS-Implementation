#ifndef PAGE_H
#define PAGE_H

//#include "MyDB_Table.h"
#include <iostream>
#include "../../Catalog/headers/MyDB_Table.h"

using namespace std;

class MyDB_Page;

typedef shared_ptr <MyDB_Page> MyDB_PagePtr;

class MyDB_BufferManager;

class MyDB_Page {

public:
	MyDB_Page(MyDB_TablePtr whichTable, long page_id, bool pinned, MyDB_BufferManager& bufferManager, long timeStamp, bool buffered);

	~MyDB_Page();

    void* getBytes();

	void wroteBytes() {
//        cout << this->bytes << endl;
        this->dirty = true;
    }

	void addRef() {
        this->refCount += 1;
    }

	void removeRef();

	void setPinned (bool pinned) {
        this->pinned = pinned;
    }

	bool getPinned () {
        return this->pinned;
    }

    long getTimeStamp() {
        return this->timeStamp;
    }

    void setTimeStamp(long timeStamp) {
        this->timeStamp = timeStamp;
    }

    MyDB_TablePtr getTable() {
        return this->whichTable;
    }

    long getPageID() {
        return this->page_id;
    }

    bool getBuffered() {
        return this->buffered;
    }

    void setBuffered(bool buffered) {
        this->buffered = buffered;
    }

    void setDirty(bool dirty) {
        this->dirty = dirty;
    }

    bool getDirty() {
        return this->dirty;
    }

private:
	friend class MyDB_BufferManager; // solve the problem of inaccessible

    MyDB_BufferManager& bufferManager;
	MyDB_TablePtr whichTable;
    long page_id;
	bool pinned;
	bool dirty;
    void* bytes;
	int refCount;
    long timeStamp;
    bool buffered;
};

#endif