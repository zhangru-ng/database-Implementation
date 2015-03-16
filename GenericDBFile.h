#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <fstream>
#include <string>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"

#include "gtest/gtest.h"

class GenericDBFile {	

friend class HeapFileTest;
friend class SortedFileTest;

FRIEND_TEST(HeapFileTest, LoadFile);
FRIEND_TEST(HeapFileTest, MoveFirst);
FRIEND_TEST(HeapFileTest, AddRecord);
FRIEND_TEST(SortedFileTest, LoadFile);
FRIEND_TEST(SortedFileTest, AddRecord);
FRIEND_TEST(SortedFileTest, MoveFirst);

protected:
	File curFile;
	Page curPage;		
    int curPageIndex;
    FileMode curMode;

public:
	GenericDBFile (); 
	virtual int Create (const char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (const char *fpath) = 0;
	virtual int Close () = 0;
	virtual void Load (Schema &myschema, const char *loadpath) = 0;
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

	
};

#endif
