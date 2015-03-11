#ifndef REL_OP_H
#define REL_OP_H

#include <string>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
private:
	DBFile *inFile;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
   	void* selectFile();
public:
	SelectFile();
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;	
	pthread_t thread;
	static void* thread_wrapper (void* arg);
   	void* selectPipe();
public:
	SelectPipe();
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Project : public RelationalOp { 
private:
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
   	void* project();
public:
	Project();
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Join : public RelationalOp { 
private:
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	Pipe *outputL;
	Pipe *outputR;
	BigQ *bqL;
	BigQ *bqR; 
	pthread_t thread;
	static void* thread_wrapper (void* arg);
	void* join();
public:
	Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class DuplicateRemoval : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Schema *mySchema;
	Pipe *output;
	BigQ *bq;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
	void* duplicateRemoval();
public:
	DuplicateRemoval();
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Sum : public RelationalOp {
private:
	Pipe *inPipe;
	Pipe *outPipe;
	Function *computeMe;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
	void* sum();
public:
	Sum() ;
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class GroupBy : public RelationalOp {
private:
	Pipe *inPipe; 
	Pipe *outPipe;
	Pipe *output;
	BigQ *bq;
	OrderMaker *groupAtts;
	Function *computeMe;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
	void* groupBy();
public:
	GroupBy();
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class WriteOut : public RelationalOp {
private:
	Pipe *inPipe;
	FILE *outFile;
	Schema *mySchema;
	pthread_t thread;
	static void* thread_wrapper (void* arg);
	void* writeOut();
public:
	WriteOut();
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

#endif
