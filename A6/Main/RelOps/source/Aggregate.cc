
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>
#include "MyDB_Schema.h"

using namespace std;

// SELECT SUM (att1 + att2), AVG (att1 + att2), att4, att6 - att8
// FROM input
// GROUP BY att4, att6 - att8

Aggregate :: Aggregate (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
		vector <pair <MyDB_AggType, string>> aggsToComputeIn,
		vector <string> groupingsIn, string selectionPredicateIn) {
	input = inputIn;
	output = outputIn;
	aggsToCompute = aggsToComputeIn;// <(SUM, "+([att1], [att2])"), (AVG, "+([att1], [att2])")>
	groupings = groupingsIn;// <"[att4]", "-([att6], [att8])">
	selectionPredicate = selectionPredicateIn;
}

void Aggregate :: run () {
	if (this->output->getTable()->getSchema()->getAtts().size() != aggsToCompute.size () + groupings.size()) {
		cout << "Error: size of [output] should be the same as the sum size of [aggsToCompute] and [groupings].";
		return;
	}

    unordered_map <size_t, void *> myHash;
	unordered_map <size_t, vector<double>> sumMap;
    unordered_map <size_t, vector<double>> avgMap;
    unordered_map <size_t, int> cntMap;

	MyDB_RecordPtr inputRec = input->getEmptyRecord();

	MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
	for (auto p : output->getTable ()->getSchema ()->getAtts ())
		mySchemaOut->appendAtt (p);
	MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);

	vector <func> groupFunc;
    for (auto g : groupings) {
        groupFunc.push_back (inputRec->compileComputation(g));
    }

    vector <pair<MyDB_AggType, func>> aggFunc;
    for (auto agg : aggsToCompute) {
        aggFunc.push_back (make_pair(agg.first, inputRec->compileComputation(agg.second)));
    }

    func pred = inputRec->compileComputation (selectionPredicate);

    // get all of the pages
	vector <MyDB_PageReaderWriter> allData;
    for (int i = 0; i < input->getNumPages(); i++) {
        MyDB_PageReaderWriter temp = (*input)[i];
        if (temp.getType () == MyDB_PageType :: RegularPage){
            allData.push_back(temp);
        }
    }
	MyDB_RecordIteratorAltPtr myIter = getIteratorAlt (allData);

	int pageCnt = 0;
	while(myIter->advance ()) {
		// hash the current record
		myIter->getCurrent (inputRec);

		// see if it is accepted by the preicate
		if (!pred ()->toBool ()) {
			continue;
		}

		// compute its hash
		size_t hashVal = 0;
		for (auto f : groupFunc) {
			hashVal ^= f ()->hash ();
		}

		if (myHash.count (hashVal) == 0) {
			cntMap[hashVal] = 1;

			// run all of the computations
			int i = 0;
			for (auto f : groupFunc) {
				combinedRec->getAtt (i++)->set (f());
			}

			i = groupFunc.size();
			for (auto p : aggFunc) {
                if (p.first == MyDB_AggType::cnt) {
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(1);
                    combinedRec->getAtt (i)->set (att);
                }
                else if(p.first == MyDB_AggType::sum){
                    combinedRec->getAtt (i)->set (p.second());
                    sumMap[hashVal].push_back(combinedRec->getAtt(i)->toDouble());
                }
                else if(p.first == MyDB_AggType::avg){
                    combinedRec->getAtt (i)->set (p.second());
                    avgMap[hashVal].push_back(combinedRec->getAtt(i)->toDouble());
                }
                i += 1;
            }

            combinedRec->recordContentHasChanged ();

            MyDB_PageReaderWriter temp = output->getPinned(pageCnt);
            void* loc = temp.appendAndReturnLocation(combinedRec);
            myHash [hashVal] = loc;

            if (loc == nullptr) {
                temp = output->getPinned(++pageCnt);
                myHash [hashVal] = temp.appendAndReturnLocation(combinedRec);
            }
		}
		else {
			combinedRec->fromBinary (myHash[hashVal]);
			cntMap[hashVal] += 1;

            int i = groupFunc.size();
            int sumCnt = 0;
            int avgCnt = 0;
            for (auto p : aggFunc) {
                if (p.first == MyDB_AggType::cnt) {
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(cntMap[hashVal]);
                    combinedRec->getAtt (i)->set (att);
                }
                else if (p.first == MyDB_AggType::sum) {
                    combinedRec->getAtt(i)->set(p.second());
                    sumMap[hashVal][sumCnt] += combinedRec->getAtt(i)->toDouble();
                    MyDB_IntAttValPtr att = make_shared<MyDB_IntAttVal>();
                    att->set(sumMap[hashVal][sumCnt]);
                    combinedRec->getAtt (i)->set (att);
                    sumCnt += 1;
                }
                else if (p.first == MyDB_AggType::avg) {
                    combinedRec->getAtt(i)->set(p.second());
                    avgMap[hashVal][avgCnt] = (avgMap[hashVal][avgCnt]*(cntMap[hashVal]-1)+combinedRec->getAtt(i)->toDouble())/cntMap[hashVal];
                    MyDB_DoubleAttValPtr att = make_shared<MyDB_DoubleAttVal>();
                    att->set(avgMap[hashVal][avgCnt]);
                    combinedRec->getAtt (i)->set (att);
                    avgCnt += 1;
                }
                i += 1;
            }

            combinedRec->recordContentHasChanged ();
            combinedRec->toBinary(myHash[hashVal]);
		}
	}
}

#endif

