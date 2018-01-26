
#ifndef CATALOG_UNIT_H
#define CATALOG_UNIT_H

//#include "MyDB_BufferManager.h"
//#include "MyDB_PageHandle.h"
//#include "MyDB_Table.h"
//#include "QUnit.h"
#include <iostream>
#include <unistd.h>
#include <vector>

#include "../../BufferMgr/headers/MyDB_BufferManager.h"
#include "../../BufferMgr/headers/MyDB_PageHandle.h"
#include "../../Catalog/headers/MyDB_Table.h"
#include "../../Qunit/headers/QUnit.h"

using namespace std;

// these functions write a bunch of characters to a string... used to produce data
void writeNums (char *bytes, size_t len, int i) {
	for (size_t j = 0; j < len - 1; j++) {
		bytes[j] = '0' + (i % 10);
	}
	bytes[len - 1] = 0;
}

void writeLetters (char *bytes, size_t len, int i) {
	for (size_t j = 0; j < len - 1; j++) {
		bytes[j] = 'i' + (i % 10);
	}
	bytes[len - 1] = 0;
}

void writeSymbols (char *bytes, size_t len, int i) {
	for (size_t j = 0; j < len - 1; j++) {
		bytes[j] = '!' + (i % 10);
	}
	bytes[len - 1] = 0;
}

int main () {
	QUnit::UnitTest qunit(cerr, QUnit::verbose);

	// UNIT TEST 1: A BIG ONE!!
	{

		// create a buffer manager
		MyDB_BufferManager myMgr (64, 16, "tempDSFSD");//here is buffer, which contains pages
		MyDB_TablePtr table1 = make_shared <MyDB_Table> ("tempTable", "foobar");//table of database


		// allocate a pinned page (1)
		cout << "allocating pinned page\n";
		MyDB_PageHandle pinnedPage = myMgr.getPinnedPage (table1, 0);
		char *bytes = (char *) pinnedPage->getBytes ();
		writeNums (bytes, 64, 0);
		pinnedPage->wroteBytes ();//dirty

		// create a bunch of pinned pages and remember them (9)
		vector <MyDB_PageHandle> myHandles;
		for (int i = 1; i < 10; i++) {
			cout << "allocating pinned page\n";
			MyDB_PageHandle temp = myMgr.getPinnedPage (table1, i);
			char *bytes = (char *) temp->getBytes ();
			writeNums (bytes, 64, i);
			temp->wroteBytes ();
			myHandles.push_back (temp);
		}

		// now forget the pages we created
		vector <MyDB_PageHandle> temp;
		myHandles = temp;//the deconstructor will be called

		// now remember 8 more pages (8)
		for (int i = 0; i < 8; i++) {
			cout << "allocating pinned page\n";
			MyDB_PageHandle temp = myMgr.getPinnedPage (table1, i);
			char *bytes = (char *) temp->getBytes ();

			// write numbers at the 0th position
			if (i == 0)
				writeNums (bytes, 64, i);
			else
                writeSymbols (bytes, 64, i);
			temp->wroteBytes ();
			myHandles.push_back (temp);
		}

		// now correctly write nums at the 0th position
		cout << "allocating unpinned page\n";
		MyDB_PageHandle anotherDude = myMgr.getPage (table1, 0);
		bytes = (char *) anotherDude->getBytes ();
		writeSymbols (bytes, 64, 0);
		anotherDude->wroteBytes ();

		// now do 90 more pages, for which we forget the handle immediately
		for (int i = 10; i < 100; i++) {
			cout << "allocating unpinned page\n";
			MyDB_PageHandle temp = myMgr.getPage (table1, i);
			char *bytes = (char *) temp->getBytes ();
			writeNums (bytes, 64, i);
			temp->wroteBytes ();
		}

		// now forget all of the pinned pages we were remembering
		vector <MyDB_PageHandle> temp2;
		myHandles = temp2;

		// now get a pair of pages and write them
		for (int i = 0; i < 100; i++) {
			cout << "allocating pinned page\n";
			MyDB_PageHandle oneHandle = myMgr.getPinnedPage ();
			char *bytes = (char *) oneHandle->getBytes ();
			writeNums (bytes, 64, i);
			oneHandle->wroteBytes ();
			cout << "allocating pinned page\n";
			MyDB_PageHandle twoHandle = myMgr.getPinnedPage ();
			writeNums (bytes, 64, i);
			twoHandle->wroteBytes ();
		}

		// make a second table
		MyDB_TablePtr table2 = make_shared <MyDB_Table> ("tempTable2", "barfoo");
		for (int i = 0; i < 100; i++) {
			cout << "allocating unpinned page\n";
			MyDB_PageHandle temp = myMgr.getPage (table2, i);
			char *bytes = (char *) temp->getBytes ();
			writeLetters (bytes, 64, i);
			temp->wroteBytes ();
		}

	}

	// UNIT TEST 2
	{
		MyDB_BufferManager myMgr (64, 16, "tempDSFSD");
		MyDB_TablePtr table1 = make_shared <MyDB_Table> ("tempTable", "foobar");

		// look up all of the pages, and make sure they have the correct numbers
		for (int i = 0; i < 100; i++) {
			MyDB_PageHandle temp = myMgr.getPage (table1, i);
			char answer[64];
			if (i < 8)
				writeSymbols (answer, 64, i);
			else
				writeNums (answer, 64, i);
			char *bytes = (char *) temp->getBytes ();
			QUNIT_IS_EQUAL (string (answer), string (bytes));
		}
	}
}

#endif


//#ifndef CATALOG_UNIT_H
//#define CATALOG_UNIT_H
//
//#include "../../BufferMgr/headers/MyDB_BufferManager.h"
//#include "../../BufferMgr/headers/MyDB_PageHandle.h"
//#include "../../Catalog/headers/MyDB_Table.h"
//#include "../../Qunit/headers/QUnit.h"
//#include <iostream>
//#include <unistd.h>
//#include <vector>
//
//using namespace std;
//
//// these functions write a bunch of characters to a string... used to produce data
//void writeNums (char *bytes, size_t len, int i) {
//    for (size_t j = 0; j < len - 1; j++) {
//        bytes[j] = '0' + (i % 10);
//    }
//    bytes[len - 1] = 0;
//}
//
//void writeLetters (char *bytes, size_t len, int i) {
//    for (size_t j = 0; j < len - 1; j++) {
//        bytes[j] = 'i' + (i % 10);
//    }
//    bytes[len - 1] = 0;
//}
//
//void writeSymbols (char *bytes, size_t len, int i) {
//    for (size_t j = 0; j < len - 1; j++) {
//        bytes[j] = '!' + (i % 10);
//    }
//    bytes[len - 1] = 0;
//}
//
//int main () {
//
//    QUnit::UnitTest qunit(cerr, QUnit::verbose);
//
//    // UNIT TEST 1: A BIG ONE!!
//    {
//
//        // create a buffer manager
//        MyDB_BufferManager myMgr (32, 32, "tempDSFSD");
//        // MyDB_TablePtr table1 = make_shared <MyDB_Table> ("tempTable", "foobar");
//
//        // // allocate a pinned page
//        // cout << "allocating pinned page\n";
//        // MyDB_PageHandle pinnedPage = myMgr.getPinnedPage (table1, 0);
//        // char *bytes = (char *) pinnedPage->getBytes ();
//        // writeNums (bytes, 32, 0);
//        // pinnedPage->wroteBytes ();
//
//
//        // // create a bunch of pinned pages and remember them
//        // vector <MyDB_PageHandle> myHandles;
//        // for (int i = 1; i < 10; i++) {
//        // 	cout << "allocating pinned page\n";
//        // 	MyDB_PageHandle temp = myMgr.getPinnedPage (table1, i);
//        // 	char *bytes = (char *) temp->getBytes ();
//        // 	writeNums (bytes, 32, i);
//        // 	temp->wroteBytes ();
//        // 	myHandles.push_back (temp);
//        // }
//
//        // // now forget the pages we created
//        // vector <MyDB_PageHandle> temp;
//        // myHandles = temp;
//
//        // // now remember 8 more pages
//        // for (int i = 0; i < 8; i++) {
//        // 	cout << "allocating pinned page\n";
//        // 	MyDB_PageHandle temp = myMgr.getPinnedPage (table1, i);
//        // 	char *bytes = (char *) temp->getBytes ();
//
//        // 	// write numbers at the 0th position
//        // 	if (i == 0)
//        // 		writeNums (bytes, 32, i);
//        // 	else
//        // 		writeSymbols (bytes, 32, i);
//        // 	temp->wroteBytes ();
//        // 	myHandles.push_back (temp);
//        // }
//
//        // // now correctly write nums at the 0th position
//        // cout << "allocating unpinned page\n";
//        // MyDB_PageHandle anotherDude = myMgr.getPage (table1, 0);
//        // bytes = (char *) anotherDude->getBytes ();
//        // writeSymbols (bytes, 32, 0);
//        // anotherDude->wroteBytes ();
//
//        // // now do 90 more pages, for which we forget the handle immediately
//        // for (int i = 10; i < 100; i++) {
//        // 	cout << "allocating unpinned page\n";
//        // 	MyDB_PageHandle temp = myMgr.getPage (table1, i);
//        // 	char *bytes = (char *) temp->getBytes ();
//        // 	writeNums (bytes, 32, i);
//        // 	temp->wroteBytes ();
//        // }
//
//        // // now forget all of the pinned pages we were remembering
//        // vector <MyDB_PageHandle> temp2;
//        // myHandles = temp2;
//
//        // // now get a pair of pages and write them
//        // for (int i = 0; i < 100; i++) {
//        // 	cout << "allocating pinned page\n";
//        // 	MyDB_PageHandle oneHandle = myMgr.getPinnedPage ();
//        // 	char *bytes = (char *) oneHandle->getBytes ();
//        // 	writeNums (bytes, 32, i);
//        // 	oneHandle->wroteBytes ();
//        // 	cout << "allocating pinned page\n";
//        // 	MyDB_PageHandle twoHandle = myMgr.getPinnedPage ();
//        // 	writeNums (bytes, 32, i);
//        // 	twoHandle->wroteBytes ();
//        // }
//
//        // make a second table
//        MyDB_TablePtr table2 = make_shared <MyDB_Table> ("tempTable2", "barfoo");
//        vector <MyDB_PageHandle> hold;
//        for (int i = 0; i < 30; i++) {
//            cout << "allocating pinned page\n";
//            MyDB_PageHandle temp = myMgr.getPinnedPage (table2, i);
//            hold.push_back(temp);
//            char *bytes = (char *) temp->getBytes ();
//            writeLetters (bytes, 32, i);
//            temp->wroteBytes ();
//        }
//
//        myMgr.unpin(hold[0]);
//
//        MyDB_PageHandle anonymous = myMgr.getPage ();
//        char *head = (char *) anonymous->getBytes ();
//        for (int i = 0; i < 32; i++)
//            head[i] = (i % 26) + 'a';
//        anonymous->wroteBytes ();
//
//        MyDB_PageHandle anonymous2 = myMgr.getPage ();
//        char *head2 = (char *) anonymous2->getBytes ();
//        for (int i = 0; i < 32; i++)
//            head2[i] = (i % 26) + '\'';
//        anonymous2->wroteBytes ();
//
//        MyDB_PageHandle temp31 = myMgr.getPage (table2, 1);
//        MyDB_PageHandle temp32 = myMgr.getPage (table2, 2);
//
//        MyDB_PageHandle anonymous3 = myMgr.getPage ();
//        char *head3 = (char *) anonymous3->getBytes ();
//        for (int i = 0; i < 32; i++)
//            head3[i] = (i % 26) + 'A';
//        anonymous3->wroteBytes ();
//
//        head2 = (char *) anonymous2->getBytes ();
//        MyDB_PageHandle temp33 = myMgr.getPage (table2, 3);
//        anonymous2 = nullptr;
//
//        head = (char *) anonymous->getBytes ();;
//
//        MyDB_PageHandle temp34 = myMgr.getPage (table2, 4);
//        MyDB_PageHandle temp35 = myMgr.getPage (table2, 5);
//
//        head = (char *) anonymous->getBytes();
//        for (int i = 0; i < 32; i++)
//            cout << head[i];
//        cout << endl;
//
//        head3 = (char *) anonymous3->getBytes();
//        for (int i = 0; i < 32; i++)
//            cout << head3[i];
//        cout << endl;
//    }
//
//    // UNIT TEST 2
//    {
//        // MyDB_BufferManager myMgr (32, 32, "tempDSFSD");
//        // MyDB_TablePtr table1 = make_shared <MyDB_Table> ("tempTable", "foobar");
//
//        // // look up all of the pages, and make sure they have the correct numbers
//        // for (int i = 0; i < 100; i++) {
//        // 	MyDB_PageHandle temp = myMgr.getPage (table1, i);
//        // 	char answer[32];
//        // 	if (i < 8)
//        // 		writeSymbols (answer, 32, i);
//        // 	else
//        // 		writeNums (answer, 32, i);
//        // 	char *bytes = (char *) temp->getBytes ();
//        // 	QUNIT_IS_EQUAL (string (answer), string (bytes));
//        // }
//
//        // MyDB_TablePtr table2 = make_shared <MyDB_Table> ("tempTable2", "barfoo");
//        // // look up all of the pages, and make sure they have the correct numbers
//        // for (int i = 0; i < 100; i++) {
//        // 	MyDB_PageHandle temp = myMgr.getPage (table2, i);
//        // 	char answer[32];
//        // 	writeLetters (answer, 32, i);
//        // 	char *bytes = (char *) temp->getBytes ();
//        // 	QUNIT_IS_EQUAL (string (answer), string (bytes));
//        // }
//    }
//}
//
//#endif
