#ifndef DBFILE_H
#define DBFILE_H

#include "HeapDBFile.h"
#include "SortedDBFile.h"

class DBFile {

friend class HeapFileTest;
FRIEND_TEST(HeapFileTest, LoadFile);
FRIEND_TEST(HeapFileTest, MoveFirst);
FRIEND_TEST(HeapFileTest, AddRecord);

private:
	GenericDBFile *myInernalPoniter;

	fType StringToEnum(const string &type);
	
public:
	DBFile (); 
	~DBFile (); 

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
