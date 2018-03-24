
#ifndef SQL_EXPRESSIONS
#define SQL_EXPRESSIONS

#include "MyDB_AttType.h"
#include "MyDB_Catalog.h"
#include <string>
#include <vector>

// create a smart pointer for database tables
using namespace std;
class ExprTree;
typedef shared_ptr <ExprTree> ExprTreePtr;

// this class encapsules a parsed SQL expression (such as "this.that > 34.5 AND 4 = 5")

// class ExprTree is a pure virtual class... the various classes that implement it are below
class ExprTree {

public:
	virtual string toString () = 0;
	virtual ~ExprTree () {};
	virtual string getType() = 0;
    virtual bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) = 0;
};

class BoolLiteral : public ExprTree {

private:
	bool myVal;
public:
	
	BoolLiteral (bool fromMe) {
		myVal = fromMe;
	}

	string toString () {
		if (myVal) {
			return "bool[true]";
		} else {
			return "bool[false]";
		}
	}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        return true;
    }
};

class DoubleLiteral : public ExprTree {

private:
	double myVal;
public:

	DoubleLiteral (double fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "double[" + to_string (myVal) + "]";
	}	

	~DoubleLiteral () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        return true;
    }
};

// this implement class ExprTree
class IntLiteral : public ExprTree {

private:
	int myVal;
public:

	IntLiteral (int fromMe) {
		myVal = fromMe;
	}

	string toString () {
		return "int[" + to_string (myVal) + "]";
	}

	~IntLiteral () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        return true;
    }
};

class StringLiteral : public ExprTree {

private:
	string myVal;
public:

	StringLiteral (char *fromMe) {
		fromMe[strlen (fromMe) - 1] = 0;
		myVal = string (fromMe + 1);
	}

	string toString () {
		return "string[" + myVal + "]";
	}

	~StringLiteral () {}

	string getType(){
		return "STRING";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        return true;
    }
};

class Identifier : public ExprTree {

private:
	string tableName;
	string attName;
	string type;
public:

	Identifier (char *tableNameIn, char *attNameIn) {
		tableName = string (tableNameIn);
		attName = string (attNameIn);
        type = "IDENTIFIER";
	}

	string toString () {
		return "[" + tableName + "_" + attName + "]";
	}	

	~Identifier () {}

	string getType(){
		return type;
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        string tableName_org = "";
        for (auto tableToProcess: tablesToProcess) {
            if (tableToProcess.second == tableName) {
                tableName_org = tableToProcess.first;
                break;
            }
        }
        if (tableName_org == "") {
            cout << "Error: Table identifier " << tableName << " is unknown." << endl;
            return false;
        }

        string attType;
        bool flag = myCatalog->getString(tableName_org + "." + attName + ".type", attType);
        if(!flag) {
            cout << "Error: The attribute " << attName << " doesn't exist in table " << tableName_org << "." << endl;
            return false;
        }

        if(attType.compare("int") == 0 || attType.compare("double") == 0) {
            type = "NUMERIC";
        }
        else if(attType.compare("string") == 0) {
            type = "STRING";
        }
        else {
            type = "BOOL";
        }
        return true;
    }
};

class MinusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	MinusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "- (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~MinusOp () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare("NUMERIC") != 0){
            cout << "Error: In Minus Operation, the lhs " << lhs->toString() << " is not Numeric Type" << endl;
            return false;
        }

        if(rhs_type.compare("NUMERIC") != 0){
            cout << "Error: In Minus Operation, the rhs " << rhs->toString() << " is not Numeric Type" << endl;
            return false;
        }

        return true;
    }
};

class PlusOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
    string type;
	
public:

	PlusOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
        type = "NUMERIC";
	}

	string toString () {
		return "+ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~PlusOp () {}

	string getType(){
		return type;
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare(rhs_type) != 0){
            cout << "Error: In Plus Operation, the type of lhs " << lhs->toString() << " and the type of rhs " << rhs->toString() + " are not the same." << endl;
            return false;
        }

        if(lhs_type.compare("STRING") == 0) {
            type = "STRING";
            return true;
        }
        else if(lhs_type.compare("NUMERIC") == 0) {
            return true;
        }
        else {
            cout << "Error: In Plus Operation, the type of lhs and rhs should be either STRING or NUMERIC." << endl;
            return false;
        }

        return true;
    }
};

class TimesOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	TimesOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "* (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~TimesOp () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare("NUMERIC") != 0){
            cout << "Error: In Time Operation, the lhs " << lhs->toString() << " is not Numeric Type" << endl;
            return false;
        }

        if(rhs_type.compare("NUMERIC") != 0){
            cout << "Error: In Time Operation, the rhs " << rhs->toString() << " is not Numeric Type" << endl;
            return false;
        }

        return true;
    }
};

class DivideOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	DivideOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "/ (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~DivideOp () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if (!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        if (rhs->toString().compare("0") == 0) {
            cout << "Error: In Divide Operation, the rhs " << rhs->toString() << " is zero" << endl;
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if (lhs_type.compare("NUMERIC") != 0) {
            cout << "Error: In Divide Operation, the lhs " << lhs->toString() << " is not Numeric Type" << endl;
            return false;
        }

        if (rhs_type.compare("NUMERIC") != 0) {
            cout << "Error: In Divide Operation, the rhs " << rhs->toString() <<"  is not Numeric Type" << endl;
            return false;
        }

        return true;
    }
};

class GtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	GtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "> (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~GtOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare(rhs_type) != 0){
            cout << "Error: In Greater Operation, the type of lhs " << lhs->toString() << " and the type of rhs " << rhs->toString() + " are not the same." << endl;
            return false;
        }

        return true;
    }
};

class LtOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	LtOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "< (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~LtOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare(rhs_type) != 0){
            cout << "Error: In Less Operation, the type of lhs " << lhs->toString() << " and the type of rhs " << rhs->toString() + " are not the same." << endl;
            return false;
        }

        return true;
    }
};

class NeqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	NeqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "!= (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~NeqOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare(rhs_type) != 0){
            cout << "Error: In Not Equal Operation, the type of lhs " << lhs->toString() << " and the type of rhs " << rhs->toString() + " are not the same." << endl;
            return false;
        }

        return true;
    }
};

class OrOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	OrOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "|| (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~OrOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare("BOOL") != 0){
            cout << "Error: In Or Operation, the type of lhs " << lhs->toString() << " is not BOOL TYPE." << endl;
            return false;
        }

        if(rhs_type.compare("BOOL") != 0){
            cout << "Error: In Or Operation, the type of rhs " << rhs->toString() << " is not BOOL TYPE." << endl;
            return false;
        }

        return true;
    }
};

class EqOp : public ExprTree {

private:

	ExprTreePtr lhs;
	ExprTreePtr rhs;
	
public:

	EqOp (ExprTreePtr lhsIn, ExprTreePtr rhsIn) {
		lhs = lhsIn;
		rhs = rhsIn;
	}

	string toString () {
		return "== (" + lhs->toString () + ", " + rhs->toString () + ")";
	}	

	~EqOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!lhs->checkQuery(myCatalog, tablesToProcess) || !rhs->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string lhs_type = lhs->getType();
        string rhs_type = rhs->getType();

        if(lhs_type.compare(rhs_type) != 0){
            cout << "Error: In Equal Operation, the type of lhs " << lhs->toString() << " and the type of rhs " << rhs->toString() + " are not the same." << endl;
            return false;
        }

        return true;
    }
};

class NotOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	NotOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "!(" + child->toString () + ")";
	}	

	~NotOp () {}

	string getType(){
		return "BOOL";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!child->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string child_type = child->getType();

        if(child_type.compare("BOOL") != 0){
            cout << "Error: In Not Operation, the type of child " << child->toString() << " is not BOOL TYPE." << endl;
            return false;
        }

        return true;
    }
};

class SumOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	SumOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "sum(" + child->toString () + ")";
	}	

	~SumOp () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!child->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string child_type = child->getType();

        if(child_type.compare("NUMERIC") != 0){
            cout << "Error: In Sum Operation, the type of child " << child->toString() << " is not NUMERIC TYPE." << endl;
            return false;
        }

        return true;
    }
};

class AvgOp : public ExprTree {

private:

	ExprTreePtr child;
	
public:

	AvgOp (ExprTreePtr childIn) {
		child = childIn;
	}

	string toString () {
		return "avg(" + child->toString () + ")";
	}	

	~AvgOp () {}

	string getType(){
		return "NUMERIC";
	}

    bool checkQuery(MyDB_CatalogPtr myCatalog, vector<pair<string,string>> tablesToProcess) {
        if(!child->checkQuery(myCatalog, tablesToProcess)){
            return false;
        }

        string child_type = child->getType();

        if(child_type.compare("NUMERIC") != 0){
            cout << "Error: In Avg Operation, the type of child " << child->toString() << " is not NUMERIC TYPE." << endl;
            return false;
        }

        return true;
    }
};

#endif
