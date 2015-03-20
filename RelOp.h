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

class RelationalOp {
protected:
	static const int buffsz = 100;	
 	int runlen;
public:	
	// default internal memory size is 100 pages for an memory consumable operation 
	RelationalOp() : runlen(100) { }
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
	int numAttsLeft;	// number of attributes in left relation	
	int numAttsRight;	// number of attributes in right relation	
	int numAttsToKeep; 	// total number of attibutes of relation after join	
	int *attsToKeep;	// array that store subscript of which attributes to keep	
	ComparisonEngine comp;	// ComparionEngine instance used in most member function
	//block-nested loop join algorithm
	void NestedLoopJoin();
	//write in memory records to file
	void WriteToFile(vector<Record> &run, DBFile &file);
	//join function for left relation as outter loop
	void JoinRecordLR(Record &left, Record &right);
	//join function for right relation as outter loop
	void JoinRecordRL(Record &right, Record &left);
	// external nested loop join
	void JoinRecInFile(DBFile &outter, DBFile &inner, int smaller);	
	// nested loop scan the relation and join records
	void FitInMemoryJoin(vector<Record> &leftRecords, vector<Record> &rightRecords, int smaller);	
	// sort-merge join algorithm
	void SortMergeJoin(OrderMaker &sortorderL, OrderMaker &sortorderR);
	// output all possible joinable tuples start from where the sort-merge join find matched tuples
	int OutputTuple(Record &left, Record &right, Pipe &outputL, Pipe &outputR, OrderMaker &sortorderL, OrderMaker &sortorderR);
	void* InternalThreadEntry();
	//function for block nested join
	// void WriteToFile(vector<Record> &run, File &file);
	// void JoinRecInFile(File &outter, File &inner, int smaller);
	// Fill M-1 blocks for the outter loop of block nested join
	// int FillBlock(File &file, Page block[], off_t &index);
public:
	Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
// pointer-to-member-function for Join class
typedef void (Join::*JoinRecFn)(Record& r1, Record& r2); 

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
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp, public Thread {
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


