#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "GenericDBFile.h"

typedef enum _FileMode{ Read, Write } FileMode;

struct SortInfo {
OrderMaker *order;
int runlen;
};

class SortedDBFile : public GenericDBFile{
FRIEND_TEST(SortedFileTest, SortedTypeHeaderFile);
private:
	OrderMaker myOrder;
	int runLength;
	FileMode curMode;
	static const int buffsz;
	Pipe *input;
	Pipe *output;
	BigQ *bq;

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
