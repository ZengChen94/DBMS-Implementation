//
// Created by Chen Zeng on 4/23/18.
//

#include "../headers/RelOps.h"
#include <sstream>
#include <unordered_map>
#include <algorithm>
#include <set>
#include <time.h>

RelOps::RelOps(SQLStatement *inSql, MyDB_CatalogPtr inCatalog, map<string, MyDB_TableReaderWriterPtr> inTables,
               MyDB_BufferManagerPtr inMgr) {
    mySql = inSql;
    myCatalog = inCatalog;
    myTables = inTables;
    myMgr = inMgr;
}

void RelOps::execute() {
    time_t start, stop;
    start = time(NULL);

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
        myTables[tablesToProcess[0].first]->getTable()->getSchema()->setAtts(tablesToProcess[0].second);  // rename
        inputTable = myTables[tableName];
    } else {
        inputTable = optimize(tablesToProcess, valuesToSelect, allDisjunctions, groupingClauses);
//        cout << "scan join finished" << endl;
        allDisjunctions.clear();
    }

//    cout << "Table result: " << endl;
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
//    cout << "*******************************************" << endl;

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
        string tmpAlias = proString.substr(start_pos + 1, end_pos - start_pos - 1);
//        cout << proString << ":" << tmpAlias << ", " << nameMap[tmpAlias] << endl;
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
    int cao = 0;
    cout << endl;
    cout << "Printing top 30 records from result set: " << endl;
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
        cao++;
        if (cao == 30)
            break;
    }
    cout << endl;


    stop = time(NULL);
    printf("Use Time: %ld second(s)\n", (stop - start));

//    if (remove("./*.bin") != 0);
//        perror("Error deleting file");
//    else;
//        puts("File successfully deleted");
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


MyDB_TableReaderWriterPtr
RelOps::optimize(vector<pair<string, string>> tablesToProcess, vector<ExprTreePtr> valuesToSelect,
                 vector<ExprTreePtr> allDisjunctions, vector<ExprTreePtr> groupingClauses) {
    int num = tablesToProcess.size();
    //sort
    vector<pair<unsigned long, int> > vec; //first: number, second: index
    for (int i = 0; i < num; i++) {
        unsigned long tmp = myTables[tablesToProcess[i].first]->getTable()->getTupleCount();
        vec.push_back(make_pair(tmp, i));
    }
    sort(vec.begin(), vec.end());
//    for (auto a:vec)
//        cout << "Order: " << a.first << "   " << a.second << endl;
    set<int> already_joined;
    int st = 2;
    if ((num == 5) && (tablesToProcess[vec[0].second].first == "nation"))
        st = 4;
    already_joined.insert(vec[st].second);
    MyDB_TableReaderWriterPtr joinedTables_ = myTables[tablesToProcess[vec[st].second].first];
    joinedTables_->getTable()->getSchema()->setAtts(tablesToProcess[vec[st].second].second);  // rename


    string t_alias = tablesToProcess[vec[st].second].second;
    vector<string> rprojections;

    set<string> tmp;

    tmp.clear();

    //right
    for (auto expr : valuesToSelect) {
        string str = expr->toString();
        while (str.find("[" + t_alias + "_") != string::npos) {
            int startpos = str.find("[" + t_alias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }

    for (auto expr : allDisjunctions) {
        string str = expr->toString();
        while (str.find("[" + t_alias + "_") != string::npos) {
            int startpos = str.find("[" + t_alias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }

    for (auto expr : groupingClauses) {
        string str = expr->toString();
        while (str.find("[" + t_alias + "_") != string::npos) {
            int startpos = str.find("[" + t_alias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }
    MyDB_SchemaPtr testOut = make_shared<MyDB_Schema>();

    for (auto &s : joinedTables_->getTable()->getSchema()->getAtts()) {
        if (tmp.find(s.first) != tmp.end()){
//            cout << "insert " << s.first << endl;
            testOut->appendAtt(s);
            rprojections.push_back("[" + s.first + "]");
        }
    }

    MyDB_TablePtr testOutTable = make_shared<MyDB_Table>("testOut1", "testOut1.bin", testOut);
    MyDB_TableReaderWriterPtr joinedTables = make_shared<MyDB_TableReaderWriter>(testOutTable, myMgr);

    RegularSelection test2(joinedTables_, joinedTables,
                           "bool[true]", rprojections);

    test2.run();

    for (int i = 1; i < vec.size(); i++) { // 添加n-1次
//        cout << "Outer loop time: " << i << endl;
//        if (remove("./output_.bin") != 0);
        bool flag = false;
        for (int j = 0; j < vec.size(); j++) {
//            cout << "Inner loop for: " << j << endl;
            if (already_joined.find(vec[j].second) != already_joined.end()) // find it
                continue;
            int cur = vec[j].second;
            string curName = tablesToProcess[cur].first;
            string curAlias = tablesToProcess[cur].second;
            //check connected
            for (auto expr : allDisjunctions) {
                pair<bool, string> joinedPair = expr->beJoined();
                if (joinedPair.first) {
                    string atts = joinedPair.second;
                    int pos = atts.find("|");
                    string leftAtt = atts.substr(0, pos);
                    string rightAtt = atts.substr(pos + 1, atts.size() - pos - 1);
                    if ((leftAtt.find(curAlias + "_") == 1) || (rightAtt.find(curAlias + "_") ==
                                                                1)) { // find the cur table, now check whether has connection within the set
                        string findAlias;
                        if ((leftAtt.find(curAlias + "_") == 1)) {
                            int findIt = rightAtt.find("_") - 1;
                            findAlias = rightAtt.substr(1, findIt);
                        } else {
                            int findIt = leftAtt.find("_") - 1;
                            findAlias = leftAtt.substr(1, findIt);
                        }
//                        cout << "Potential match for: " << findAlias << endl;
                        for (auto est: already_joined) {
                            string existName = tablesToProcess[est].first;
                            string existAlias = tablesToProcess[est].second;
                            if (findAlias == existAlias) { // find the match
//                                cout << "Exact match for: " << findAlias << endl;
                                joinedTables = joinTwoTables(joinedTables, existName, existAlias, curName, curAlias,
                                                             valuesToSelect, allDisjunctions, groupingClauses);
                                already_joined.insert(cur);
                                flag = true;
                                break;
                            }
                        }
                        if (flag)
                            break;
                    }
                }
            }
            if (flag)
                break;
        }
    }
    return joinedTables;
}


MyDB_TableReaderWriterPtr
RelOps::joinTwoTables(MyDB_TableReaderWriterPtr leftTable, string leftTableName, string leftTableAlias,
                      string rightTableName, string rightTableAlias, vector<ExprTreePtr> valuesToSelect,
                      vector<ExprTreePtr> allDisjunctions, vector<ExprTreePtr> groupingClauses) {

    cout << "Joining table " << leftTableAlias << "  +  " << rightTableAlias << endl;

    MyDB_TableReaderWriterPtr rightTable_ = myTables[rightTableName];

//            rightTable->loadFromTextFile("./"+rightTableName+".tbl");

//    leftTable->getTable()->getSchema()->setAtts(leftTableAlias);  // rename
    rightTable_->getTable()->getSchema()->setAtts(rightTableAlias);  // rename





    vector<string> rprojections;

    set<string> tmp;

    tmp.clear();
    //right
    for (auto expr : valuesToSelect) {
        string str = expr->toString();
        while (str.find("[" + rightTableAlias + "_") != string::npos) {
            int startpos = str.find("[" + rightTableAlias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }

    for (auto expr : allDisjunctions) {
        string str = expr->toString();
        while (str.find("[" + rightTableAlias + "_") != string::npos) {
            int startpos = str.find("[" + rightTableAlias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }

    for (auto expr : groupingClauses) {
        string str = expr->toString();
        while (str.find("[" + rightTableAlias + "_") != string::npos) {
            int startpos = str.find("[" + rightTableAlias + "_") + 1;
            int endpos = str.find("]");
            string res = str.substr(startpos, endpos - startpos);
            tmp.insert(res);
            str = str.substr(endpos + 1);
        }
    }

    MyDB_SchemaPtr testOut = make_shared<MyDB_Schema>();

    for (auto &s : rightTable_->getTable()->getSchema()->getAtts()) {
        if (tmp.find(s.first) != tmp.end()){
            testOut->appendAtt(s);
            rprojections.push_back("[" + s.first + "]");
        }
    }

    MyDB_TablePtr testOutTable = make_shared<MyDB_Table>("testOut", "testOut.bin", testOut);
    MyDB_TableReaderWriterPtr rightTable = make_shared<MyDB_TableReaderWriter>(testOutTable, myMgr);

    RegularSelection test2(rightTable_, rightTable,
                           "bool[true]", rprojections);

    test2.run();

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

    MyDB_TablePtr myTableOut = make_shared<MyDB_Table>("output_" + leftTableAlias + "_" + rightTableAlias,
                                                       "output_" + leftTableAlias + "_" + rightTableAlias + ".bin",
                                                       mySchemaOut);
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(myTableOut, myMgr);

    vector<pair<string, string>> equalityChecks;
    vector<string> finalPredicates;
    vector<string> leftPredicates;
    vector<string> rightPredicates;

    for (auto expr : allDisjunctions) {
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

    if (finalPredicates.empty()) {
        cout << "NO COMMON PREDICATE, CANNOT JOIN!" << endl;
//        leftTable->getTable()->getSchema()->resetAtts();  // reset
//        rightTable->getTable()->getSchema()->resetAtts();  // reset
//        if (remove("./testOut.bin") != 0);
        return nullptr;
    }

    ScanJoin scan(leftTable, rightTable, outputTable,
                  finalSelectionPredicate, allProjections, equalityChecks, leftSelectionPredicate,
                  rightSelectionPredicate);


    cout << leftTableAlias << "   " << rightTableAlias << "   " << finalSelectionPredicate << "    "
         << leftSelectionPredicate << "   " << rightSelectionPredicate << endl << endl;

    for (auto t:allProjections)
        cout << t << endl;
    cout << endl;
    for (auto t:equalityChecks)
        cout << t.first << "   " << t.second << endl;

    scan.run();

    cout << "Table result: " << endl;
    MyDB_RecordPtr tem3p = outputTable->getEmptyRecord();
    MyDB_RecordIteratorAltPtr myIter3 = outputTable->getIteratorAlt();
    int counter = 0;
    while (myIter3->advance()) {
        myIter3->getCurrent(tem3p);
        cout << tem3p << endl;
        counter++;
        if (counter == 12)
            break;
    }
    cout << "*******************************************" << endl;


//    leftTable->getTable()->getSchema()->resetAtts();  // reset
//    rightTable->getTable()->getSchema()->resetAtts();  // reset
//    if (remove("./testOut.bin") != 0);
    return outputTable;
}