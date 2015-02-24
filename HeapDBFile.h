#ifndef HEAPDBFILE_H
#define HEAPDBFILE_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "GenericDBFile.h"
#include "gtest/gtest.h"


class HeapDBFile: public GenericDBFile{

friend class HeapFileTest;
FRIEND_TEST(HeapFileTest, LoadFile);
FRIEND_TEST(HeapFileTest, MoveFirst);
FRIEND_TEST(HeapFileTest, AddRecord);
void EnumToString(fType f_type, char *type);	

public:
	HeapDBFile (); 
	~HeapDBFile (); 

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
