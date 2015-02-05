#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "gtest/gtest.h"

typedef enum {heap, sorted, tree} fType;


class DBFile {

friend class DBFileTest;
FRIEND_TEST(DBFileTest, LoadFile);
FRIEND_TEST(DBFileTest, MoveFirst);
FRIEND_TEST(DBFileTest, AddRecord);

private:    
	File curFile;
	Page curPage;		
    int curPageIndex;
public:
	DBFile (); 

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
