//
// Created by Chen Zeng on 4/23/18.
//

#include "../headers/RelOps.h"
#include <sstream>
#include <unordered_map>
#include <algorithm>
#include <set>

RelOps::RelOps(SQLStatement *inSql, MyDB_CatalogPtr inCatalog, map<string, MyDB_TableReaderWriterPtr> inTables,
               MyDB_BufferManagerPtr inMgr) {
    mySql = inSql;
    myCatalog = inCatalog;
    myTables = inTables;
    myMgr = inMgr;
}

void RelOps::execute() {
    unordered_map<string, string> nameMap;
    SFWQuery myQuery = mySql->myQuery;
    vector<pair<string, string>> tablesToProcess = myQuery.tablesToProcess;
    vector<ExprTreePtr> valuesToSelect = myQuery.valuesToSelect;
    vector<ExprTreePtr> allDisjunctions = myQuery.allDisjunctions;
    vector<ExprTreePtr> groupingClauses = myQuery.groupingClauses;

    MyDB_TableReaderWriterPtr inputTable = nullptr;
    string tableName = tablesToProcess[0].first;
    string tableAlias = tablesToProcess[0].second;

    for (int i = 0; i < tablesToProcess.size(); i++) {
        string tmpName = tablesToProcess[i].first;
        string tmpAlias = tablesToProcess[i].second;
        nameMap[tmpAlias] = tmpName;
    }

    if (tablesToProcess.size() == 1) {
        inputTable = myTables[tableName];
//        inputTable->loadFromTextFile("./"+tableName+".tbl");
    }
        // start of join
    else {
        string table_name;
        string table_alias;
        int num = tablesToProcess.size();
        //sort
        vector<pair<unsigned long, int> > vec; //first: number, second: index
        for (int i = 0; i < num; i++) {
            unsigned long tmp = myTables[tablesToProcess[i].first]->getTable()->getTupleCount();
//            cout << "for " << i << "+" << tmp << endl;
            vec.push_back(make_pair(tmp, i));
        }
        set<int> set;
        sort(vec.begin(), vec.end());

        int s = 0;
        while (set.size() < num) {
            int r;
            MyDB_TableReaderWriterPtr leftTable;
            string leftTableName, leftTableAlias;
            if (s == 0) {
                leftTableName = tablesToProcess[vec[0].second].first;
                leftTableAlias = tablesToProcess[vec[0].second].second;
                leftTable = myTables[leftTableName];
                set.insert(vec[0].second);
                r = vec[1].second;
//                leftTable->loadFromTextFile("./" + leftTableName + ".tbl");
            } else {
                leftTableName = table_name;
                leftTableAlias = table_alias;
                leftTable = inputTable;
                for (; s < vec.size() - 1; s++) {
                    if (set.find(vec[s].second) == set.end()) // not found
                        break;
                }
                r = vec[s++].second;
            }
            string rightTableName = tablesToProcess[r].first;
            string rightTableAlias = tablesToProcess[r].second;
            nameMap[rightTableAlias] = rightTableName;
            MyDB_TableReaderWriterPtr rightTable = myTables[rightTableName];

//            rightTable->loadFromTextFile("./"+rightTableName+".tbl");

            MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();

            vector<string> allProjections;
            vector<pair<string, MyDB_AttTypePtr>> leftAtts = leftTable->getTable()->getSchema()->getAtts();
            vector<pair<string, MyDB_AttTypePtr>> rightAtts = rightTable->getTable()->getSchema()->getAtts();

            for (auto &p : leftAtts) {
                mySchemaOut->appendAtt(p);
                allProjections.push_back("[" + p.first + "]");
            }
            for (auto &p : rightAtts) {
                mySchemaOut->appendAtt(p);
                allProjections.push_back("[" + p.first + "]");
            }

            MyDB_TablePtr myTableOut = make_shared<MyDB_Table>("output_" + to_string(s), "output_" + to_string(s) + ".bin", mySchemaOut);
            MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(myTableOut, myMgr);

            vector<ExprTreePtr> disjunctions = myQuery.allDisjunctions;
            vector<pair<string, string>> equalityChecks;
            vector<string> finalPredicates;
            vector<string> leftPredicates;
            vector<string> rightPredicates;

            for (auto expr : disjunctions) {
                pair<bool, string> joinedPair = expr->beJoined();
                if (joinedPair.first) {
                    string atts = joinedPair.second;
                    int pos = atts.find("|");
                    string leftAtt = atts.substr(0, pos);
                    string rightAtt = atts.substr(pos + 1, atts.size() - pos - 1);
                    if ((leftAtt.find(leftTableAlias + "_") == 1) && (rightAtt.find(rightTableAlias + "_") == 1)) {
                        finalPredicates.push_back(expr->toString());
                        equalityChecks.emplace_back(make_pair(leftAtt, rightAtt));
                    } else if ((rightAtt.find(leftTableAlias + "_") == 1) &&
                               (leftAtt.find(rightTableAlias + "_") == 1)) {
                        finalPredicates.push_back(expr->toString());
                        equalityChecks.emplace_back(make_pair(rightAtt, leftAtt));
                    }
                } else {
                    if (joinedPair.second == leftTableAlias)
                        leftPredicates.push_back(expr->toString());
                    else if (joinedPair.second == rightTableAlias)
                        rightPredicates.push_back(expr->toString());
                }
            }

            string finalSelectionPredicate = constructPredicate(finalPredicates);
            string leftSelectionPredicate = constructPredicate(leftPredicates);
            string rightSelectionPredicate = constructPredicate(rightPredicates);

            if (finalPredicates.empty()){
                continue;
            }

            ScanJoin scan(leftTable, rightTable, outputTable,
                          finalSelectionPredicate, allProjections, equalityChecks, leftSelectionPredicate,
                          rightSelectionPredicate);


//            cout << leftTableAlias << "   " << rightTableAlias << "   " << finalSelectionPredicate << "    "
//                 << leftSelectionPredicate << "   " << rightSelectionPredicate << endl << endl;

//            for (auto t:allProjections)
//                cout << t << endl;
//            cout << endl;
//            for (auto t:equalityChecks)
//                cout << t.first << "   " << t.second << endl;

            scan.run();

            inputTable = outputTable;
            table_name = rightTableName;
            table_alias = rightTableAlias;
            set.insert(r);
            s = 1;

        }
        cout << "scan join finished" << endl;
        allDisjunctions.clear();
    }
// end of join


//    MyDB_RecordPtr tem3p = inputTable->getEmptyRecord();
//    MyDB_RecordIteratorAltPtr myIter3 = inputTable->getIteratorAlt();
//    int counter = 0;
//    while (myIter3->advance()) {
//        myIter3->getCurrent(tem3p);
//        cout << tem3p << endl;
//        counter++;
//        if (counter == 30)
//            break;
//    }

    vector<ExprTreePtr> aggVector;
    vector<ExprTreePtr> nonAggVector;
    unordered_map<int, int> orderMap; //oldIndex, newIndex
    int cntNonAgg = 0;
    int cntAgg = 0;
    for (auto v :valuesToSelect) {
        if (v->getType().compare("SUM") != 0 && v->getType().compare("AVG") != 0) {
            cntNonAgg += 1;
        }
    }
    cntAgg = cntNonAgg;
    cntNonAgg = 0;
    int cnt = 0;
    for (auto v : valuesToSelect) {
        if (v->getType().compare("SUM") == 0 || v->getType().compare("AVG") == 0) {
            aggVector.push_back(v);
            orderMap[cnt++] = cntAgg++;
        } else {
            nonAggVector.push_back(v);
            orderMap[cnt++] = cntNonAgg++;
        }
    }
    valuesToSelect.clear();

    valuesToSelect.insert(valuesToSelect.end(), nonAggVector.begin(), nonAggVector.end());
    valuesToSelect.insert(valuesToSelect.end(), aggVector.begin(), aggVector.end());

    MyDB_SchemaPtr mySchemaOut = make_shared<MyDB_Schema>();

    vector<string> projections;
    vector<pair<MyDB_AggType, string>> aggsToCompute;
    vector<string> groupings;
    for (auto v : valuesToSelect) {
        projections.push_back(v->toString());
        string proString = v->toString();
        size_t start_pos = proString.find("[");
        size_t end_pos = proString.find("_");
        string tmpAlias = proString.substr(start_pos+1, end_pos-start_pos-1);
        cout << proString << ":" << tmpAlias << ", " << nameMap[tmpAlias] << endl;
        mySchemaOut->appendAtt(v->getAttPair(myCatalog, nameMap[tmpAlias]));
        if (v->getType().compare("SUM") == 0) {
            aggsToCompute.push_back(make_pair(MyDB_AggType::SUM, v->toString().substr(3)));
        } else if (v->getType().compare("AVG") == 0) {
            aggsToCompute.push_back(make_pair(MyDB_AggType::AVG, v->toString().substr(3)));
        }

        if (v->getType().compare("SUM") != 0 && v->getType().compare("AVG") != 0) {
            groupings.push_back(v->toString());
        }
    }

//// TODO
//    mySchemaOut->appendAtt(make_pair("", make_shared<MyDB_IntAttType>()));
//    mySchemaOut->appendAtt(make_pair("", make_shared<MyDB_DoubleAttType>()));

//    for (auto g : groupingClauses) {
//        groupings.push_back(cutPrefix(g->toString(), tableAlias));
//    }

    MyDB_TablePtr myTableOut = make_shared<MyDB_Table>("output", "output.bin", mySchemaOut);
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(myTableOut, myMgr);

    vector<string> allDisjunctionsString;
    for (auto a : allDisjunctions) {
        allDisjunctionsString.push_back(a->toString());
    }
    string selectionPredicate = constructPredicate(allDisjunctionsString);
//    if (allDisjunctions.size() == 1) {
//        selectionPredicate = allDisjunctions[0]->toString();
//    } else {
//        selectionPredicate = "&& (" + allDisjunctions[0]->toString() + ", " +
//                             allDisjunctions[1]->toString() + ")";
//        for (int i = 2; i < allDisjunctions.size(); i++) {
//            selectionPredicate =
//                    "&& (" + selectionPredicate + ", " + allDisjunctions[i]->toString() + ")";
//        }
//    }

//    for (auto a : aggsToCompute) {
//        if (a.first == MyDB_AggType::SUM)
//            cout << "AGG: SUM, " + a.second << endl;
//        else if (a.first == MyDB_AggType::AVG)
//            cout << "AGG: AVG, " + a.second << endl;
//        else
//            cout << "AGG: ???, " + a.second << endl;
//    }
//    for (auto g : groupings) {
//        cout << "GROUPING: " + g << endl;
//    }
//    cout << "SELECTION: " << selectionPredicate << endl;
//    return;

    if (aggsToCompute.size() == 0) {
        RegularSelection regularSelection(inputTable, outputTable, selectionPredicate, projections);
        regularSelection.run();
    } else {
        Aggregate aggregate(inputTable, outputTable, aggsToCompute, groupings, selectionPredicate);
        aggregate.run();

    }

    MyDB_RecordPtr temp = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr myIter = outputTable->getIteratorAlt();
    while (myIter->advance()) {
        stringstream buffer;
        myIter->getCurrent(temp);
        buffer << temp;
//        cout << buffer.str() << endl;
        vector<string> res = splitRes(buffer.str());
        for (int i = 0; i < res.size(); i++) {
            cout << res[orderMap[i]] << "|";
        }
        cout << endl;
    }

    if (remove("./output.bin") != 0)
        perror("Error deleting file");
    else
        puts("File successfully deleted");
}

//string RelOps::cutPrefix(string input, string alias) {
//    size_t start_pos = input.find("[" + alias + "_" + alias + "_");
//    while (start_pos != std::string::npos) {
//        input = input.substr(0, start_pos) + "[" + alias + "_" + input.substr(start_pos + 3 + alias.length() * 2);
//        start_pos = input.find("[" + alias + "_" + alias + "_");
//    }
//    return input;
//}

vector<string> RelOps::splitRes(string input) {
    vector<string> res;
    size_t start_pos = input.find("|");
    while (start_pos != std::string::npos) {
        res.push_back(input.substr(0, start_pos));
        input = input.substr(start_pos + 1);
        start_pos = input.find("|");
    }
    return res;
}

string RelOps::constructPredicate(vector<string> allPredicates) {
    string res;
//    cout << "predicate size is " << allPredicates.size() << endl;
    if (allPredicates.size() == 0)
        res = "bool[true]";
    else if (allPredicates.size() == 1)
        res = allPredicates[0];
    else {
        res = "&& (" + allPredicates[0] + ", " + allPredicates[1] + ")";
        for (int i = 2; i < allPredicates.size(); i++)
            res = "&& (" + res + ", " + allPredicates[i] + ")";
    }
//    cout << "predicate is " << res << endl;
    return res;
}