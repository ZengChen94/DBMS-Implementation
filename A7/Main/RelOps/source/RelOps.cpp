//
// Created by Chen Zeng on 4/23/18.
//

#include "RelOps.h"

RelOps :: RelOps(SQLStatement *inSql, MyDB_CatalogPtr inCatalog, map <string, MyDB_TableReaderWriterPtr> inTables, MyDB_BufferManagerPtr inMgr){
    mySql = inSql;
    myCatalog = inCatalog;
    myTables = inTables;
    myMgr = inMgr;
}

void RelOps::execute() {
    SFWQuery myQuery = mySql->myQuery;
    vector <pair <string, string>> tablesToProcess = myQuery.tablesToProcess;
    vector <ExprTreePtr> valuesToSelect = myQuery.valuesToSelect;
    vector <ExprTreePtr> allDisjunctions = myQuery.allDisjunctions;
    vector <ExprTreePtr> groupingClauses = myQuery.groupingClauses;

    MyDB_TableReaderWriterPtr inputTable = nullptr;
    string tableName = tablesToProcess[0].first;
    string tableAlias = tablesToProcess[0].second;
    if (tablesToProcess.size() == 1){
        inputTable = myTables[tableName];
    }
    else if(tablesToProcess.size() == 2){
//        string leftTableName = tablesToProcess[1].first;
//        string leftTableAlias = tablesToProcess[1].second;
//        string rightTableName = tablesToProcess[0].first;
//        string rightTableAlias = tablesToProcess[0].second;
//
//        MyDB_TableReaderWriterPtr leftTable = myTables[leftTableName];
//        MyDB_TableReaderWriterPtr rightTable = myTables[rightTableName];
//
//        MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
//
//        vector <string> allProjections;
//        vector <pair <string, MyDB_AttTypePtr>> leftAtts = leftTable->getTable()->getSchema()->getAtts();
//        for (auto &p : leftAtts){
//            mySchemaOut->appendAtt(p);
//            allProjections.push_back("["+p.first+"]");
//        }
//        vector <pair <string, MyDB_AttTypePtr>> rightAtts = rightTable->getTable()->getSchema()->getAtts();
//        for (auto &p : rightAtts){
//            mySchemaOut->appendAtt(p);
//            allProjections.push_back("["+p.first+"]");
//        }
//
//        MyDB_TablePtr myTableOut = make_shared <MyDB_Table> ("output", "output.bin", mySchemaOut);
//        MyDB_TableReaderWriterPtr outputTable = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);
        cout << "Error: JOIN is not implemented yet." << endl;
        return;
    }
    else {
        cout << "Error: can only support no more than 2 tables." << endl;
        return;
    }

    MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();

    vector <string> projections;
    vector <pair <MyDB_AggType, string>> aggsToCompute;
    vector <string> groupings;
    for (auto v : valuesToSelect) {
        projections.push_back(cutPrefix(v->toString(), tableAlias));
        mySchemaOut->appendAtt(v->getAttPair(myCatalog, tableName));
//        cout << v->getAttPair(myCatalog, tableName).second->toString() << endl;

        if(v->getType().compare("SUM") == 0){
            aggsToCompute.push_back(make_pair(MyDB_AggType::SUM, cutPrefix(v->toString(), tableAlias).substr(3)));
        }
        else if(v->getType().compare("AVG") == 0){
            aggsToCompute.push_back(make_pair(MyDB_AggType::AVG, cutPrefix(v->toString(), tableAlias).substr(3)));
        }
        else{
            groupings.push_back(cutPrefix(v->toString(), tableAlias));
        }
    }

    MyDB_TablePtr myTableOut = make_shared <MyDB_Table> ("output", "output.bin", mySchemaOut);
    MyDB_TableReaderWriterPtr outputTable = make_shared <MyDB_TableReaderWriter> (myTableOut, myMgr);

    string selectionPredicate;
    if (allDisjunctions.size() == 1) {
        selectionPredicate = cutPrefix(allDisjunctions[0]->toString(), tableAlias);
    }
    else {
        selectionPredicate = "&& (" + cutPrefix(allDisjunctions[0]->toString(), tableAlias) + ", " + cutPrefix(allDisjunctions[1]->toString(), tableAlias) + ")";
        for (int i = 2; i < allDisjunctions.size(); i++){
            selectionPredicate = "&& (" + selectionPredicate + ", " + cutPrefix(allDisjunctions[i]->toString(), tableAlias) + ")";
        }
    }

    cout << selectionPredicate << endl;

    if(aggsToCompute.size() == 0) {
        RegularSelection regularSelection (inputTable, outputTable, selectionPredicate, projections);
        regularSelection.run();
    }
    else {
        Aggregate aggregate (inputTable, outputTable, aggsToCompute, groupings, selectionPredicate);
        aggregate.run();
    }

    MyDB_RecordPtr temp = outputTable->getEmptyRecord ();
    MyDB_RecordIteratorAltPtr myIter = outputTable->getIteratorAlt ();
    while (myIter->advance ()) {
        myIter->getCurrent (temp);
        cout << temp << "\n";
    }
}

//string RelOps::cutPrefix(string input, string alias) {
//    size_t start_pos = input.find("["+alias+"_");
//    return input.substr(0, start_pos) + "[" + input.substr(start_pos+2+alias.length());
//}

string RelOps::cutPrefix(string input, string alias) {
    size_t start_pos = input.find("["+alias+"_"+alias+"_");
    while (start_pos != std::string::npos) {
        input = input.substr(0, start_pos) + "[" + alias + "_" + input.substr(start_pos+3+alias.length()*2);
        start_pos = input.find("["+alias+"_"+alias+"_");
    }
    return input;
}