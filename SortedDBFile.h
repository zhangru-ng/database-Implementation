#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"

//the current mode of the file: Read or Write
typedef enum _FileMode{ Read, Write } FileMode;

typedef struct _SortInfo {
	OrderMaker *order;
	int runlen;
}SortInfo;

class SortedDBFile : public GenericDBFile{
	FRIEND_TEST(SortedFileTest, SortedTypeHeaderFile);
private:
	OrderMaker myOrder;			//the OrderMaker used to sort this file	
	int runLength;				//the run lenght used to sort this file
	FileMode curMode;			//this file's current mode	
	string filename;
	BigQ *bq;					//the internal bigQ of this file
	Pipe *input;				//input pipe for internal bigQ
	Pipe *output;				//output pipe for internal bigQ
	static const int buffsz = 100;		//the buffer size of pipe

	OrderMaker queryOrder;		//the queryOrder built by file ordermaker and the input CNF
	bool isNewQuery;			//true if start a new query, used to reduce to overhead of repeatedly cnf query GetNext 

	//merge the file's internal BigQ with its other sorted data when change from write to read or close the file
	void MergeSortedParts();
	//simple GetNext sub-function used when there's no attribute in query OrderMaker
	int GetNextRecord(Record &fetchme, CNF &cnf, Record &literal);
	//use the file OrderMaker myOrder and SearchOrder derive from input CNF to built query OrderMaker
	void MakeQueryOrder(OrderMaker &SearchOrder);
	//use in conjunction with query OrderMaker to speed up GetNext
	int BinarySearch(Record &literal, Record &outRec);
	//after the binary search locate match record, examine record one-by-one to find accepted record
	int FindAcceptedRecord(Record &fetchme, Record &literal, CNF &cnf);

public:
	SortedDBFile (); 
	~SortedDBFile ();

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
