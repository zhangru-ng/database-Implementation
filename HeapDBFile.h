#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include "GenericDBFile.h"


class HeapDBFile: public GenericDBFile{

friend class HeapFileTest;
	FRIEND_TEST(HeapFileTest, LoadFile);
	FRIEND_TEST(HeapFileTest, MoveFirst);
	FRIEND_TEST(HeapFileTest, AddRecord);

public:
	HeapDBFile (); 

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
