#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"

typedef enum _FileMode{ Read, Write } FileMode;

typedef struct _SortInfo {
	OrderMaker *order;
	int runlen;
}SortInfo;

class SortedDBFile : public GenericDBFile{
	FRIEND_TEST(SortedFileTest, SortedTypeHeaderFile);
private:
	OrderMaker myOrder;
	int runLength;
	FileMode curMode;
	static const int buffsz = 100;
	Pipe *input;
	Pipe *output;
	BigQ *bq;

	OrderMaker queryOrder;
	bool isNewQuery;

	void MergeSortedParts();
	int GetNextRecord(Record &fetchme, CNF &cnf, Record &literal);
	void MakeQueryOrder(OrderMaker &SearchOrder);
	int BinarySearch(Record &literal, Record &outRec);
	int FindAcceptedRecord(Record &fetchme, Record &literal, CNF &cnf);

public:
	SortedDBFile (); 
	~SortedDBFile ();

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
	void Load (Schema &myschema, char *loadpath);
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
