
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
//#include "MyDB_PageReaderWriter.h"
//#include "MyDB_TableReaderWriter.h"

#include "../../DatabaseTable/headers/MyDB_PageReaderWriter.h"
#include "../../DatabaseTable/headers/MyDB_TableReaderWriter.h"
#include "../../DatabaseTable/headers/MyDB_TableRecIterator.h"

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr forMe, MyDB_BufferManagerPtr myBuffer): myTable(forMe), myBuffer(myBuffer){
	if (this->myTable->lastPage() == -1) {
		this->myTable->setLastPage(0);
		shared_ptr <MyDB_PageReaderWriter> pageReaderWriter = make_shared <MyDB_PageReaderWriter> (myTable, myBuffer, 0);
		pageReaderWriter->clear();
	}
}

MyDB_PageReaderWriter &MyDB_TableReaderWriter :: operator [] (size_t i) {
	shared_ptr <MyDB_PageReaderWriter> pageReaderWriter;
	while (this->myTable->lastPage() < i) {
		this->myTable->setLastPage(this->myTable->lastPage()+1);
		pageReaderWriter = make_shared <MyDB_PageReaderWriter> (this->myTable, this->myBuffer, this->myTable->lastPage());
		pageReaderWriter->clear();
	}
	pageReaderWriter = make_shared <MyDB_PageReaderWriter> (this->myTable, this->myBuffer, i);
	return *pageReaderWriter;
}

MyDB_RecordPtr MyDB_TableReaderWriter :: getEmptyRecord () {
	return make_shared <MyDB_Record> (this->myTable->getSchema());
}

MyDB_PageReaderWriter &MyDB_TableReaderWriter :: last () {
	return (*this)[this->myTable->lastPage()];
}


void MyDB_TableReaderWriter :: append (MyDB_RecordPtr appendMe){
	shared_ptr <MyDB_PageReaderWriter> pageReaderWriter = make_shared <MyDB_PageReaderWriter> (this->myTable, this->myBuffer, this->myTable->lastPage ());
	if (!pageReaderWriter->append(appendMe)) {
		myTable->setLastPage (myTable->lastPage() + 1);
		pageReaderWriter = make_shared <MyDB_PageReaderWriter> (this->myTable, this->myBuffer, this->myTable->lastPage());
		pageReaderWriter->clear();
	}
}

void MyDB_TableReaderWriter :: loadFromTextFile (string fileName) {
	this->myTable->setLastPage(0);
	shared_ptr <MyDB_PageReaderWriter> pageReaderWriter = make_shared <MyDB_PageReaderWriter> (myTable, myBuffer, 0);
	pageReaderWriter->clear();

	ifstream fin;
	fin.open(fileName);
	string line;
	MyDB_RecordPtr emptyRec = getEmptyRecord ();
	while (getline(fin, line)) {
		emptyRec->fromString(line);
		append(emptyRec);
	}
	fin.close ();
}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr iterateIntoMe) {
	return make_shared <MyDB_TableRecIterator> (*this, this->myTable, iterateIntoMe);
}

void MyDB_TableReaderWriter :: writeIntoTextFile (string fileName) {
	ofstream fout;
	fout.open(fileName);
	MyDB_RecordPtr emptyRec = getEmptyRecord();
	MyDB_RecordIteratorPtr recIter = getIterator(emptyRec);
	while (recIter->hasNext()) {
		recIter->getNext();
	}
	fout.close ();
}

#endif

