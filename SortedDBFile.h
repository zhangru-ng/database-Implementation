#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"
#include <memory>

#define KEY_NOT_FOUND -1

using std::unique_ptr;

struct SortInfo {
	OrderMaker *order;
	int runlen;
};

class SortedDBFile : public GenericDBFile{
	FRIEND_TEST(SortedFileTest, SortedTypeHeaderFile);
private:
	OrderMaker myOrder;			//the OrderMaker used to sort this file	
	int runLength;				//the run lenght used to sort this file			
	string filename;
	unique_ptr<BigQ> bq;					//the internal bigQ of this file
	unique_ptr<Pipe> input;				//input pipe for internal bigQ
	unique_ptr<Pipe> output;				//output pipe for internal bigQ
	static const int buffsz = 100;		//the buffer size of pipe
	OrderMaker queryOrder;		//the queryOrder built by file ordermaker and the input CNF
	bool isNewQuery;			//true if start a new query, used to reduce to overhead of repeatedly cnf query GetNext 

	//initial the internal BigQ of this file
	void initBigQ();
	//merge BigQ with sorted data when change from write to read or close the file
	void MergeSortedParts();
	//subroutine of MergeSortedParts, compare record in bigQ with that in sorted file to merge them
	void MergeRecord(Page &tempPage, File &tempFile, int &tempIndex);
	//add record to temporary file when merge bigQ with sorted file 
	void AddRecord(Record &tempRec, Page &tempPage, File &tempFile, int &tempIndex);
	//simple GetNext sub-function used when there's no attribute in query OrderMaker
	int GetNextRecord(Record &fetchme, CNF &cnf, Record &literal);
	//use the file OrderMaker myOrder and SearchOrder derive from input CNF to built query OrderMaker
	void MakeQueryOrder(OrderMaker &SearchOrder, int *litOrder);
	//use in conjunction with query OrderMaker to speed up GetNext
	int BinarySearch(Record &literal, Record &outRec, int *litOrder);
	//after the binary search locate match record, examine record one-by-one to find accepted record
	int FindAcceptedRecord(Record &fetchme, Record &literal, CNF &cnf, int *litOrder);

public:
	SortedDBFile (); 

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();
	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
