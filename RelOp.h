#ifndef REL_OP_H
#define REL_OP_H

#include <vector>
#include <string>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Thread.h"

using namespace std;

class RelationalOp {
protected:
	static const int buffsz = 100;	
 	int runlen;
public:	
	RelationalOp() : runlen(20) { }
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;

};

// Both RelationlOP and Thread are abstract base class, moreover, they are
// completely irrelavent class, thus use multiple inheritance

class SelectFile : public RelationalOp, public Thread { 
private:
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
   	void* InternalThreadEntry();
public:
	SelectFile();
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp, public Thread {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;	
   	void* InternalThreadEntry();
public:
	SelectPipe();
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Project : public RelationalOp, public Thread { 
private:
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
   	void* InternalThreadEntry();
public:
	Project();
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Join : public RelationalOp, public Thread { 
private:
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	// number of attributes in left relation
	int numAttsLeft;
	// number of attributes in right relation
	int numAttsRight;
	// total number of attibutes of relation after join
	int numAttsToKeep; 
	int OutputTuple(Record &left, Record &right, Pipe &outputL, Pipe &outputR, OrderMaker &sortorderL, OrderMaker &sortorderR, int *attsToKeep);
	void BlockNestedJoin();
	void SortMergeJoin(OrderMaker &sortorderL, OrderMaker &sortorderR);
	void* InternalThreadEntry();
public:
	Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class DuplicateRemoval : public RelationalOp, public Thread {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	void* InternalThreadEntry();
public:
	DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Sum : public RelationalOp, public Thread {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	int intSum;
	double doubleSum;
	string IntSum();
	string DoubleSum();
	void* InternalThreadEntry();
public:
	Sum() ;
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp, public Thread {
private:
	Pipe *inPipe; 
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;
	int intSum;
	double doubleSum;
	void* InternalThreadEntry();
	void GroupInt(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *Atts, Schema &sumSchema);
	void GroupDouble(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *Atts, Schema &sumSchema);
public:
	GroupBy();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp, public Thread {
private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;	
	void* InternalThreadEntry();
public:
	WriteOut();
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif
