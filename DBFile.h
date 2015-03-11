#ifndef DBFILE_H
#define DBFILE_H

#include "HeapDBFile.h"
#include "SortedDBFile.h"

class DBFile {

friend class HeapFileTest;
friend class SortedFileTest;

FRIEND_TEST(HeapFileTest, LoadFile);
FRIEND_TEST(HeapFileTest, MoveFirst);
FRIEND_TEST(HeapFileTest, AddRecord);
FRIEND_TEST(SortedFileTest, LoadFile);
FRIEND_TEST(SortedFileTest, AddRecord);
FRIEND_TEST(SortedFileTest, MoveFirst);

private:
	GenericDBFile *myInernalPoniter;
	//check if the internal pointer is initialize
	bool AssertInit();
public:
	DBFile (); 

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
