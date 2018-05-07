//
// Created by Chen Zeng on 4/23/18.
//

#ifndef A7_RELOPS_H
#define A7_RELOPS_H

#include "ParserTypes.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_Catalog.h"

#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"

class RelOps {

public:
    RelOps(SQLStatement *inSql, MyDB_CatalogPtr inCatalog, map<string, MyDB_TableReaderWriterPtr> inTables,
           MyDB_BufferManagerPtr inMgr);

    void execute();

//    string cutPrefix(string input, string alias);

    vector<string> splitRes(string input);

    string constructPredicate(vector<string> allPredicates);

    MyDB_TableReaderWriterPtr
    optimize(vector<pair<string, string>> tablesToProcess, vector<ExprTreePtr> valuesToSelect,
             vector<ExprTreePtr> disjunctions,vector<ExprTreePtr> groupingClauses);

    MyDB_TableReaderWriterPtr
    joinTwoTables(MyDB_TableReaderWriterPtr leftTable, string existName, string existAlias, string curName,
                  string curAlias,vector<ExprTreePtr> valuesToSelect,
                  vector<ExprTreePtr> disjunctions,vector<ExprTreePtr> groupingClauses);

private:
    SQLStatement *mySql;
    MyDB_CatalogPtr myCatalog;
    map<string, MyDB_TableReaderWriterPtr> myTables;
    MyDB_BufferManagerPtr myMgr;
};

#endif //A7_RELOPS_H
