
#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

//#include "MyDB_PageHandle.h"
//#include "MyDB_Table.h"

#include <map>
#include <list>
#include <vector>
#include "./MyDB_PageHandle.h"
#include "../../Catalog/headers/MyDB_Table.h"

using namespace std;

class MyDB_BufferManager {

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS
    void remove(MyDB_Page &page);

    void process(MyDB_Page &page);

    bool evictLRU();

private:

	// YOUR STUFF HERE
	vector<void*> buffer;
	size_t pageSize;
	size_t numPages;
	string tempFile;

    long anonIndex;// keep track of the id of anonymous pages

    long globalTimeStamp;

    map<pair<MyDB_TablePtr, long>, MyDB_PagePtr> page_map;// pair, page
};

#endif


