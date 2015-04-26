#ifndef REL_OP_H
#define REL_OP_H

#include <vector>
#include <string>
#include <algorithm>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "Thread.h"

using namespace std;
//marco for compare which relation is smaller in block-nested Join
#define LEFT 0
#define RIGHT 1

class RelationalOp : public Thread {
protected:
	static const int buffsz = 100;	
 	int runlen;
public:	
	// default internal memory size is 100 pages for an memory consumable operation 
	RelationalOp() : runlen(100) { }
	// blocks the caller until the particular relational operator 
	// has run to completion
	void WaitUntilDone ();
	// tell us how much internal memory the operation can use
	void Use_n_Pages (int n);

};

// Both RelationlOP and Thread are abstract base class, moreover, they are
// completely irrelavent class, thus use multiple inheritance

class SelectFile : public RelationalOp { 
private:
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
   	void* InternalThreadEntry();
public:
	SelectFile();
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
};

class SelectPipe : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;	
   	void* InternalThreadEntry();
public:
	SelectPipe();
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
};

class Project : public RelationalOp { 
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
};

class Join : public RelationalOp { 
private:
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;	
	int numAttsLeft;	// number of attributes in left relation	
	int numAttsRight;	// number of attributes in right relation	
	int numAttsToKeep; 	// total number of attibutes of relation after join	
	int *attsToKeep;	// array that store subscript of which attributes to keep	
	ComparisonEngine comp;	// ComparionEngine instance used in most member function
	//block-nested loop join algorithm
	void NestedLoopJoin();
	// internal nested loop join
	void InMemoryJoin (vector<Record> &leftRecords, Pipe *inPipeR, Record &tempRecR);	
	void JoinRecInMemory(vector<Record> &leftRecords, Record &right);
	// external nested loop join
	void InFileJoin (DBFile &file, Pipe *inPipeR, Record &tempRecR);
	void JoinRecInFile (DBFile &file, Record &right);
	// sort-merge join algorithm
	void SortMergeJoin(OrderMaker &sortorderL, OrderMaker &sortorderR);
	// output all possible joinable tuples start from where the sort-merge join find matched tuples
	int OutputTuple(Record &left, Record &right, Pipe &outputL, Pipe &outputR, OrderMaker &sortorderL, OrderMaker &sortorderR);
	void* InternalThreadEntry();
public:
	Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
};

class DuplicateRemoval : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	void* InternalThreadEntry();
public:
	DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};

class Sum : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;	
	int intSum;			//store aggregation result for int attribute	
	double doubleSum;	//store aggregation result for double attribute
	//compute aggregation for int attributes
	string IntSum();
	//compute aggregation for double attributes
	string DoubleSum();
	void* InternalThreadEntry();
public:
	Sum() ;
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
};

class GroupBy : public RelationalOp {
private:
	Pipe *inPipe; 
	Pipe *outPipe;
	OrderMaker *groupAtts;
	Function *computeMe;	
	int intSum;			//store aggregation result for int attribute	
	double doubleSum;	//store aggregation result for double attribute
	//group by int attirbutes
	void GroupInt(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *Atts, Schema &sumSchema);
	//group by double attirbutes
	void GroupDouble(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *Atts, Schema &sumSchema);	
	void* InternalThreadEntry();	
public:
	GroupBy();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
};

class WriteOut : public RelationalOp {
private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;
	int outputMode;	
	void* InternalThreadEntry();
public:
	WriteOut();
	void Run (Pipe &inPipe, Schema &mySchema, int outputMode);
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema, int outputMode);
};

#endif


