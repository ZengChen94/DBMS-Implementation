
#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

//#include "MyDB_BufferManager.h"
#include <string>

#include "../../BufferMgr/headers/MyDB_BufferManager.h"

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<sys/types.h>
#include<sys/stat.h>
#include<fcntl.h>

using namespace std;

MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
    pair<MyDB_TablePtr, long> whichPage = make_pair(whichTable, i);
    if (this->page_map.count(whichPage) == 0) {
        if (this->buffer.size() == 0) {
            this->evict();
        }
        if (this->buffer.size() == 0) {// cannot evict successfully
            return nullptr;
        }
        else {
            MyDB_PagePtr page = make_shared <MyDB_Page> (whichTable, i, false, *this, this->globalTimeStamp, false);
            this->globalTimeStamp += 1;
            this->page_map[whichPage] = page;
            MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(page);
            return handle;
        }
    }
	else {
        this->page_map[whichPage]->setTimeStamp(this->globalTimeStamp);// update the timeStamp of page
        this->globalTimeStamp += 1;
        MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(this->page_map[whichPage]);// make another handle to the page
        return handle;
    }
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
    if (this->buffer.size() == 0) {
        this->evict();
    }
    if (this->buffer.size() == 0) {
        return nullptr;
    }
    pair<MyDB_TablePtr, long> whichPage = make_pair(nullptr, this->tempIndex);
    MyDB_PagePtr page = make_shared <MyDB_Page> (nullptr, this->tempIndex, false, *this, this->globalTimeStamp, false);
    this->tempIndex += 1;
    this->globalTimeStamp += 1;
    this->page_map[whichPage] = page;
    MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(page);
    return handle;
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
    pair<MyDB_TablePtr, long> whichPage = make_pair(whichTable, i);
    if (this->page_map.count(whichPage) == 0) {
        if (this->buffer.size() == 0) {
            this->evict();
        }
        if (this->buffer.size() == 0) {
            return nullptr;
        }
        else {
            MyDB_PagePtr page = make_shared <MyDB_Page> (whichTable, i, true, *this, this->globalTimeStamp, false);
            this->globalTimeStamp += 1;
            this->page_map[whichPage] = page;
            MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(page);
            return handle;
        }
    }
    else {
        this->page_map[whichPage]->setTimeStamp(this->globalTimeStamp);
        this->globalTimeStamp += 1;
        MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(this->page_map[whichPage]);
        return handle;
    }
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
    if (this->buffer.size() == 0) {
        this->evict();
    }
    if (this->buffer.size() == 0) {
        return nullptr;
    }
    pair<MyDB_TablePtr, long> whichPage = make_pair(nullptr, this->tempIndex);
    MyDB_PagePtr page = make_shared <MyDB_Page> (nullptr, this->tempIndex, true, *this, this->globalTimeStamp, false);
    this->tempIndex += 1;
    this->globalTimeStamp += 1;
    this->page_map[whichPage] = page;
    MyDB_PageHandle handle = make_shared <MyDB_PageHandleBase>(page);
    return handle;
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
    unpinMe->getPage()->setPinned(false);
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile): pageSize(pageSize), numPages(numPages), tempFile(tempFile){
    this->tempIndex = 0;
    this->globalTimeStamp = 0;
	for (size_t i = 0; i < numPages; i++) {
		buffer.push_back(malloc(pageSize));
	}
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
    for (auto page : buffer) {
        free(page);
    }
}

void MyDB_BufferManager :: remove(MyDB_Page &page){
    if (page.dirty) {
        // TODO
        // load from page.bytes to (page.whichTable, page.page_id)
        int fd;
        if (page.whichTable == nullptr){
            fd = open(tempFile.c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
        }
        else{
            fd = open(page.whichTable->getStorageLoc().c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
        }
        lseek(fd, page.getPageID() * this->pageSize, SEEK_SET);
        write(fd, page.bytes, this->pageSize);
        close(fd);
        page.setDirty(false);
    }
    page.setBuffered(false);
    page.bytes = nullptr;
    buffer.push_back(page.bytes);
}

void MyDB_BufferManager :: process(MyDB_Page &page){
    if (page.getBuffered() == false) {
        if (this->buffer.size() == 0) {
            this->evict();
        }
        if (this->buffer.size() == 0) {
            return;
        }
        page.setBuffered(true);
        page.bytes = this->buffer[this->buffer.size()-1];
        this->buffer.pop_back();
        // TODO
        // load from (page.whichTable, page.page_id) to page.bytes
        int fd;
        if (page.whichTable == nullptr){
            fd = open(this->tempFile.c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
        }
        else{
            fd = open(page.whichTable->getStorageLoc().c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
        }
        lseek(fd, page.getPageID() * this->pageSize, SEEK_SET);
        read(fd, page.bytes, this->pageSize);
        close(fd);
    }
}

// LRU Algorithm
void MyDB_BufferManager :: evict(){
    long minTimeStamp = LONG_MAX;
    // search for minTimeStamp
    for (auto key = this->page_map.begin(); key != this->page_map.end(); ++key) {
        MyDB_PagePtr page = key->second;
        if (page->getTimeStamp() < minTimeStamp && page->getPinned() == false) {
            minTimeStamp = page->getTimeStamp();
        }
    }
    // remove
    for (auto key = this->page_map.begin(); key != this->page_map.end(); ++key) {
        MyDB_PagePtr page = key->second;
        if (page->getTimeStamp() == minTimeStamp) {
            if (page->dirty) {
                // TODO
                // load from page->bytes to (page->whichTable, page->page_id)
                int fd;
                if (page->whichTable == nullptr){
                    fd = open(tempFile.c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
                }
                else{
                    fd = open(page->whichTable->getStorageLoc().c_str (), O_CREAT | O_RDWR | O_SYNC, 0666);
                }
                lseek(fd, page->getPageID() * this->pageSize, SEEK_SET);
                write(fd, page->bytes, this->pageSize);
                close(fd);
                page->setDirty(false);
            }
            page->setBuffered(false);
            page->bytes = nullptr;
            buffer.push_back(page->bytes);
        }
    }
}
	
#endif


