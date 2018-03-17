
#ifndef BPLUS_C
#define BPLUS_C

#include "../../Record/headers/MyDB_INRecord.h"
#include "../headers/MyDB_BPlusTreeReaderWriter.h"
#include "../headers/MyDB_PageReaderWriter.h"
#include "../headers/MyDB_PageListIteratorSelfSortingAlt.h"
#include "../headers/RecordComparator.h"

// Chris
MyDB_BPlusTreeReaderWriter::MyDB_BPlusTreeReaderWriter(string orderOnAttName, MyDB_TablePtr forMe,
                                                       MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter(forMe,
                                                                                                                myBuffer) {

    // find the ordering attribute
    auto res = forMe->getSchema()->getAttByName(orderOnAttName);

    // remember information about the ordering attribute
    orderingAttType = res.second;
    whichAttIsOrdering = res.first;

    // and the root location
    rootLocation = getTable()->getRootLocation();
}

// Chen
MyDB_RecordIteratorAltPtr
MyDB_BPlusTreeReaderWriter::getSortedRangeIteratorAlt(MyDB_AttValPtr low, MyDB_AttValPtr high) {
    return getRangeIteratorAltHelper(low, high, true);
}

// Chen
MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter::getRangeIteratorAlt(MyDB_AttValPtr low, MyDB_AttValPtr high) {
    return getRangeIteratorAltHelper(low, high, false);
}

// Chen
MyDB_RecordIteratorAltPtr
MyDB_BPlusTreeReaderWriter::getRangeIteratorAltHelper(MyDB_AttValPtr low, MyDB_AttValPtr high, bool sortOrNot) {
    vector<MyDB_PageReaderWriter> pages;
    discoverPages(this->rootLocation, pages, low, high);
    MyDB_RecordPtr lhs = getEmptyRecord();
    MyDB_RecordPtr rhs = getEmptyRecord();
    MyDB_RecordPtr myRec = getEmptyRecord();

    MyDB_INRecordPtr IN_low = getINRecord();
    MyDB_INRecordPtr IN_high = getINRecord();

    IN_low->setKey(low);
    IN_high->setKey(high);

    function<bool()> comparator = buildComparator(lhs, rhs);
    function<bool()> lowComparator = buildComparator(myRec, IN_low);
    function<bool()> highComparator = buildComparator(IN_high, myRec);

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comparator, myRec, lowComparator,
                                                            highComparator, sortOrNot);
}

// Chen
bool MyDB_BPlusTreeReaderWriter::discoverPages(int whichPage, vector<MyDB_PageReaderWriter> &list, MyDB_AttValPtr low,
                                               MyDB_AttValPtr high) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if (page.getType() == MyDB_PageType::RegularPage) {
        list.push_back(page);
        return true; // return true if it is leaf
    } else {
        MyDB_RecordIteratorAltPtr pageIter = page.getIteratorAlt();

        MyDB_INRecordPtr myRec = getINRecord();
        MyDB_INRecordPtr IN_low = getINRecord();
        MyDB_INRecordPtr IN_high = getINRecord();
        IN_low->setKey(low);
        IN_high->setKey(high);
        function<bool()> lowComparator = buildComparator(myRec, IN_low);
        function<bool()> highComparator = buildComparator(IN_high, myRec);

        bool flag = false;

        while (pageIter->advance()) {
            pageIter->getCurrent(myRec);

            if (!lowComparator())
                flag = true;

            if (flag) {
                discoverPages(myRec->getPtr(), list, low, high);
            }

            if (highComparator())
                break;
        }

        return false;
    }
}

void MyDB_BPlusTreeReaderWriter::append(MyDB_RecordPtr rec) {
    if (rootLocation == -1) {
        MyDB_PageReaderWriter rootPage = (*this)[++rootLocation];
        rootPage.setType(MyDB_PageType::DirectoryPage);
        MyDB_INRecordPtr ptr = getINRecord();

        int newPageLoc = getTable()->lastPage() + 1;
        getTable()->setLastPage(newPageLoc);
        MyDB_PageReaderWriter leafPage = (*this)[newPageLoc];
        leafPage.clear();
        leafPage.setType(MyDB_PageType::RegularPage);
        ptr->setPtr(newPageLoc);
        rootPage.append(ptr);
    }
    MyDB_RecordPtr ret = append(this->rootLocation, rec);
    if (ret != nullptr) {
        //check if it's root
//        cout << "change root" << endl;
        int newRoot = getTable()->lastPage() + 1;
        getTable()->setLastPage(newRoot);
        MyDB_PageReaderWriter newRootPage = (*this)[newRoot];
        newRootPage.clear();
        newRootPage.setType(MyDB_PageType::DirectoryPage);
        MyDB_INRecordPtr ptr = getINRecord();
        ptr->setPtr(rootLocation);
        //add 2 inner node into root
        newRootPage.append(ret);
        newRootPage.append(ptr);
        rootLocation = newRoot;
//        cout << "change roottttttttttttttt" << endl;
    }
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::split(MyDB_PageReaderWriter splitMe, MyDB_RecordPtr addMe) {
    MyDB_INRecordPtr newPtr = getINRecord();
    //init 2 new page
    int newPageLoc = getTable()->lastPage() + 1;
    getTable()->setLastPage(newPageLoc);
    MyDB_PageReaderWriter newPage = (*this)[newPageLoc];
    newPage.clear();

    int newPageLoc_ = getTable()->lastPage() + 1;
//    getTable()->setLastPage(newPageLoc_);
    MyDB_PageReaderWriter newPage_ = (*this)[newPageLoc_];
    newPage_.clear();

    //split a regular page(leaf)
    if (splitMe.getType() == MyDB_PageType::RegularPage) {
//        cout << "split regular!" << endl;

        newPage.setType(MyDB_PageType::RegularPage);
        newPage_.setType(MyDB_PageType::RegularPage);
        //sort
        MyDB_RecordPtr rec1 = getEmptyRecord();
        MyDB_RecordPtr rec2 = getEmptyRecord();
        function<bool()> myComparator = buildComparator(rec1, rec2);
        splitMe.sortInPlace(myComparator, rec1, rec2);

        MyDB_RecordIteratorAltPtr recordIter = splitMe.getIteratorAlt();
        size_t size = splitMe.getPageSize();
        size_t counter = 0;
        int n = 0;
        MyDB_RecordPtr record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            n++;
        }
        int mid = n / 2 - 1, count = 0;
        recordIter = splitMe.getIteratorAlt();
        record = getEmptyRecord();
        bool flag = true;
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            if (count == mid && flag){
                flag = false;
                newPtr->setPtr(newPageLoc);
                newPtr->setKey(getKey(record));
            }
            if (count <= mid)
                newPage.append(record);
            else
                newPage_.append(record);
            count++;
        }
        function<bool()> myComp = buildComparator(addMe, newPtr);
        if (myComp()) {
            newPage.append(addMe);
            newPage.sortInPlace(myComparator, rec1, rec2);
        } else {
            newPage_.append(addMe);
            newPage_.sortInPlace(myComparator, rec1, rec2);
        }

        //copy back
        splitMe.clear();
        recordIter = newPage_.getIteratorAlt();
        record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            splitMe.append(record);
        }
        newPage_.clear();
//        cout << "   After split: "<<endl;
//        printHelper(newPageLoc, 1);
//        cout << "   *********************** "<<endl;
//        printHelper(whichPage, 1);

    }//split an inner node page
    else {
//        cout << "split inner!" << endl;
//        printTree();

        newPage.setType(MyDB_PageType::DirectoryPage);
        newPage_.setType(MyDB_PageType::DirectoryPage);
        //sort
        MyDB_INRecordPtr in1 = getINRecord();
        MyDB_INRecordPtr in2 = getINRecord();
        function<bool()> myComparator = buildComparator(in1, in2);
        splitMe.sortInPlace(myComparator, in1, in2);
        //get 1/2
        MyDB_RecordIteratorAltPtr recordIter = splitMe.getIteratorAlt();
        size_t size = splitMe.getPageSize();
        size_t counter = 0;

        int n = 0;
        MyDB_RecordPtr record = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            n++;
        }
        int mid = n / 2 - 1, count = 0;
        recordIter = splitMe.getIteratorAlt();
        record = getINRecord();
        bool flag = true;
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            if (count == mid && flag){
                flag = false;
                newPtr->setPtr(newPageLoc);
                newPtr->setKey(getKey(record));
            }
            if (count <= mid)
                newPage.append(record);
            else
                newPage_.append(record);
            count++;
        }
        function<bool()> myComp = buildComparator(addMe, newPtr);
        if (myComp()) {
            newPage.append(addMe);
            newPage.sortInPlace(myComparator, in1, in2);
        } else {
            newPage_.append(addMe);
            newPage_.sortInPlace(myComparator, in1, in2);
        }
        //copy back
        splitMe.clear();
        splitMe.setType(MyDB_PageType::DirectoryPage);
        recordIter = newPage_.getIteratorAlt();
        record = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            splitMe.append(record);
        }
        newPage_.clear();
//        printHelper(newPageLoc, 1);
    }
//    append(newPtr);
//    append(addMe);
    return newPtr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
//    printTree();
    MyDB_PageReaderWriter page = (*this)[whichPage];
    if (appendMe->getSchema() == nullptr) { // inner node record
//        cout << "inner record" << endl;
//        MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();
//        MyDB_INRecordPtr record = getINRecord();
//        while (recordIter->advance()) {
//            recordIter->getCurrent(record);
//            function<bool()> myComparator = buildComparator(appendMe, record);
//            if (myComparator) {
//                MyDB_PageReaderWriter nextLevel = (*this)[record->getPtr()];
//                if (nextLevel.getType() ==
//                    MyDB_PageType::RegularPage) { //nextlevel is regular page, cannot insert an in record
//                    cout << "nextlevel is regular page, cannot insert an in record" << endl;
//                    if (page.append(appendMe))
//                        return nullptr;
//                    else
//                        return split(whichPage, appendMe);
//                } else {
//                    cout << "nextlevel is inner page, go deep" << endl;
//                    return append(record->getPtr(), appendMe);
//                }
//            }
//        }
        if (page.append(appendMe)) {
            MyDB_INRecordPtr in1 = getINRecord();
            MyDB_INRecordPtr in2 = getINRecord();
            function<bool()> myComparator = buildComparator(in1, in2);
            page.sortInPlace(myComparator, in1, in2);
            return nullptr;
        } else {
            auto f = split((*this)[whichPage], appendMe);
//            cout << "split inner page ";
//            cout << whichPage << endl;
            return f;
        }
    } else { // regular record
//        cout << "regular record" << endl;
        if (page.getType() == MyDB_PageType::RegularPage) {
            if (page.append(appendMe)) {
                return nullptr;
            } else {
                auto f = split((*this)[whichPage], appendMe);
                return f;
            }
        } else {
            MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();
            MyDB_INRecordPtr record = getINRecord();
            while (recordIter->advance()) {
                recordIter->getCurrent(record);
                function<bool()> myComparator = buildComparator(appendMe, record);
                if (myComparator()) {
                    auto f = append(record->getPtr(), appendMe);
                    if (f == nullptr) {
                        return nullptr;
                    } else {
//                        cout << whichPage << endl;
//                        cout << rootLocation << endl;
                        return append(whichPage, f);
                    }
                }
            }
//            cout << "ERR" << endl;
        }
    }
}

// Chris
MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter::getINRecord() {
    return make_shared<MyDB_INRecord>(orderingAttType->createAttMax());
}

// Chen
void MyDB_BPlusTreeReaderWriter::printTree() {
    printHelper(this->rootLocation, 0);
}

// Chen
void MyDB_BPlusTreeReaderWriter::printHelper(int whichPage, int depth) {
    MyDB_PageReaderWriter page = (*this)[whichPage];
    MyDB_RecordIteratorAltPtr recordIter = page.getIteratorAlt();

    // leaf
    if (page.getType() == MyDB_PageType::RegularPage) {
        MyDB_RecordPtr record = getEmptyRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(record);
            cout << depth << "\t";
            cout << record << "\t";
        }
        cout << "\n";
    }
        // inner node
    else {
        MyDB_INRecordPtr inRecord = getINRecord();
        while (recordIter->advance()) {
            recordIter->getCurrent(inRecord);
            printHelper(inRecord->getPtr(), depth + 1);
            cout << depth << "\t";
            cout << inRecord->getKey() << "\t";
        }
        cout << "\n";
    }
}

// Chris
MyDB_AttValPtr MyDB_BPlusTreeReaderWriter::getKey(MyDB_RecordPtr fromMe) {

    // in this case, got an IN record
    if (fromMe->getSchema() == nullptr)
        return fromMe->getAtt(0)->getCopy();

        // in this case, got a data record
    else
        return fromMe->getAtt(whichAttIsOrdering)->getCopy();
}

// Chris
function<bool()> MyDB_BPlusTreeReaderWriter::buildComparator(MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

    MyDB_AttValPtr lhAtt, rhAtt;

    // in this case, the LHS is an IN record
    if (lhs->getSchema() == nullptr) {
        lhAtt = lhs->getAtt(0);

        // here, it is a regular data record
    } else {
        lhAtt = lhs->getAtt(whichAttIsOrdering);
    }

    // in this case, the LHS is an IN record
    if (rhs->getSchema() == nullptr) {
        rhAtt = rhs->getAtt(0);

        // here, it is a regular data record
    } else {
        rhAtt = rhs->getAtt(whichAttIsOrdering);
    }

    // now, build the comparison lambda and return
    if (orderingAttType->promotableToInt()) {
        return [lhAtt, rhAtt] { return lhAtt->toInt() < rhAtt->toInt(); };
    } else if (orderingAttType->promotableToDouble()) {
        return [lhAtt, rhAtt] { return lhAtt->toDouble() < rhAtt->toDouble(); };
    } else if (orderingAttType->promotableToString()) {
        return [lhAtt, rhAtt] { return lhAtt->toString() < rhAtt->toString(); };
    } else {
        cout << "This is bad... cannot do anything with the >.\n";
        exit(1);
    }
}


#endif