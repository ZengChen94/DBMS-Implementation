
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

//static const MyDB_TableReaderWriterPtr outputIn = outputIn;

#include "../headers/Aggregate.h"
#include "../../Record/headers/MyDB_Record.h"
#include "../../DatabaseTable/headers/MyDB_PageReaderWriter.h"
#include "../../DatabaseTable/headers/MyDB_TableReaderWriter.h"
#include "../headers/SortMergeJoin.h"
#include "../../DatabaseTable/headers/Sorting.h"
#include <iostream>
#include <vector>
#include <unordered_map>
#include <stdio.h>

SortMergeJoin::SortMergeJoin(MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
                             MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
                             vector<string> projectionsIn,
                             pair<string, string> equalityCheckIn, string leftSelectionPredicateIn,
                             string rightSelectionPredicateIn) {
    output = outputIn;
    finalSelectionPredicate = finalSelectionPredicateIn;
    projections = projectionsIn;
    equalityCheck = equalityCheckIn;
    leftTable = leftInputIn;
    rightTable = rightInputIn;
    leftSelectionPredicate = leftSelectionPredicateIn;
    rightSelectionPredicate = rightSelectionPredicateIn;
}

void SortMergeJoin::run() {

    int runSize = int(leftTable->getBufferMgr()->numPages / 2); //?

    MyDB_RecordPtr temp = leftTable->getEmptyRecord();
    MyDB_RecordPtr temp2 = leftTable->getEmptyRecord();
    function<bool()> myComp = buildRecordComparator(temp, temp2, equalityCheck.first);
    MyDB_RecordIteratorAltPtr left_iter = buildItertorOverSortedRuns(runSize, *leftTable,
                                                                     myComp, temp, temp2, leftSelectionPredicate);
    MyDB_RecordPtr temp_ = rightTable->getEmptyRecord();
    MyDB_RecordPtr temp2_ = rightTable->getEmptyRecord();
    function<bool()> myComp_ = buildRecordComparator(temp_, temp2_, equalityCheck.second);
    MyDB_RecordIteratorAltPtr right_iter = buildItertorOverSortedRuns(runSize, *rightTable,
                                                                      myComp_, temp_, temp2_, rightSelectionPredicate);


    // and get the schema that results from combining the left and right records
    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();
    for (auto &p : leftTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);
    for (auto &p : rightTable->getTable()->getSchema()->getAtts())
        mySchemaOut->appendAtt(p);

    // get the combined record
    MyDB_RecordPtr combinedRec = make_shared<MyDB_Record>(mySchemaOut);
    combinedRec->buildFrom(temp, temp_);

    // now, get the final predicate over it
    func finalPredicate = combinedRec->compileComputation(finalSelectionPredicate);

    // and get the final set of computatoins that will be used to buld the output record
    vector<func> finalComputations;
    for (string s : projections) {
        finalComputations.push_back(combinedRec->compileComputation(s));
    }

    // this is the output record
    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    // compare funcs
    func left = combinedRec->compileComputation(" < (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func right = combinedRec->compileComputation(" > (" + equalityCheck.first + ", " + equalityCheck.second + ")");
    func equal = combinedRec->compileComputation(" == (" + equalityCheck.first + ", " + equalityCheck.second + ")");

    //do the merge
    vector<MyDB_PageReaderWriter> leftContainer;
    vector<MyDB_PageReaderWriter> rightContainer;
    MyDB_PageReaderWriter leftPage(true, *(leftTable->getBufferMgr()));
    MyDB_PageReaderWriter rightPage(true, *(rightTable->getBufferMgr()));

    function<bool()> myComp2 = buildRecordComparator(temp2, temp,
                                                     equalityCheck.first); // a reverse operator to check equality in left table

    if (left_iter->advance() && right_iter->advance()) {
        while (true) {
            bool flag = false;
            left_iter->getCurrent(temp);
            right_iter->getCurrent(temp_);
            if (left()->toBool()) {
                if (!left_iter->advance())
                    flag = true;
            } else if (right()->toBool()) {
                if (!right_iter->advance())
                    flag = true;
            } else if (equal()->toBool()) {
                // add current LHS, RHS
                leftPage.clear();
                rightPage.clear();
                leftContainer.clear();
                rightContainer.clear();
                leftContainer.push_back(leftPage);
                rightContainer.push_back(rightPage);

                //find all equalities in left
                while (true) {
                    left_iter->getCurrent(temp2);   // use temp2 to maintain the same temp, and check equality
                    if (!myComp() && !myComp2()) {
                        if (!leftPage.append(temp2)) {
                            MyDB_PageReaderWriter newPage(true, *(leftTable->getBufferMgr()));
                            leftPage = newPage;
                            leftContainer.push_back(leftPage);
                            leftPage.append(temp2);
                        }
                    } else
                        break;
                    if (!left_iter->advance()) {
                        flag = true;
                        break;
                    }
                }

                //find all equalities in right
                while (true) {
                    right_iter->getCurrent(temp_);  // use temp_ to maintain the equality with temp, instead of temp2_!!!
                    if (equal()->toBool()) {
                        if (!rightPage.append(temp_)) {
                            MyDB_PageReaderWriter newPage(true, *(rightTable->getBufferMgr()));
                            rightPage = newPage;
                            rightContainer.push_back(rightPage);
                            rightPage.append(temp_);
                        }
                    } else
                        break;
                    if (!right_iter->advance()) {
                        flag = true;
                        break;
                    }
                }

                //merge all the matches and output
                MyDB_RecordIteratorAltPtr myIterLeft;
                if (leftContainer.size() == 1)
                    myIterLeft = leftContainer[0].getIteratorAlt();
                else
                    myIterLeft = getIteratorAlt(leftContainer);

                while (myIterLeft->advance()) {
                    myIterLeft->getCurrent(temp);
                    MyDB_RecordIteratorAltPtr myIterRight;
                    if (rightContainer.size() == 1)
                        myIterRight = rightContainer[0].getIteratorAlt();
                    else
                        myIterRight = getIteratorAlt(rightContainer);
                    while (myIterRight->advance()) {
                        myIterRight->getCurrent(temp_);
                        // check final predicate
                        if (finalPredicate()->toBool()) {
                            int i = 0;
                            for (auto &f : finalComputations) {
                                outputRec->getAtt(i++)->set(f());
                            }
                            outputRec->recordContentHasChanged();
                            output->append(outputRec);
                        }
                    }
                }
            }
            if (flag)
                break;
        }
    }
}

#endif