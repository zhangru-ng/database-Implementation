#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}


void enumToString(fType f_type, char *type){
	switch(f_type){
		case heap:
			strcpy(type, "heap");
		case sorted:
			strcpy(type, "sorted");
		case tree:
			strcpy(type, "tree");
	}
}
//The following function is used to actually create the file. The first
//parameter is a text string that tells where the binary data is 
//physically to be located. If there is meta-data(type of file, etc),
//store in an associated text file(.header). The second parameter is an
//enumeration with three possible values: heap, sorted and tree. The 
//return value is a one on success and a zero on failure
int DBFile::Create (char *f_path, fType f_type, void *startup) {
	FILE * fp;
	char s[200],type[8];
		
	//create the binary DBfile
	curFile.Open (0, f_path);
	
	//create the associated text file
	strcpy(s, f_path);
	strcat(s,".header");
	if(	(fp= fopen (s,"w")) == NULL){
		cerr << "Can't create associated file for" << f_path << "\n";
		return 0;
	}
	enumToString(f_type, type);
	fputs(type, fp);
	fclose(fp);	
	return 1;
}



//This function assumes that the DBFile already exists and has previous 
//been created and then closed. The one parameter to this function is 
//simply the physical location fo the file.The return value is a one on 
//success and a zero on failure(open auxiliary text file at startup)
int DBFile::Open (char *f_path) {
	FILE * fp;
	char s[200],type[8];
	
	//open the exist DBfile
	curFile.Open (1, f_path);
	
	//open the exist associate file
	strcpy(s, f_path);
	strcat(s,".header");
	if(	(fp= fopen (s,"r")) == NULL){
		cerr << "Can't open associated file for" << f_path << "\n";
		return 0;
	}
	fgets(type, 6, fp);
	fclose(fp);	
	return 1;
}

//Simply close the file. The return value is a one on success and a zero 
//on failure
int DBFile::Close () {
	curFile.Close ();
	return 1;
}

//This function bulk the DBFile instance from a text file, appending new
//data to it using the SuckNextRecord function from Record.h. 
//The character string passed to Load is the name of the data file to 
//bulk load
void DBFile::Load (Schema &f_schema, char *loadpath) {
	FILE *tableFile = fopen (loadpath, "r");
	Record rec;
	int counter = 0;
	if(tableFile == NULL){
		cerr << "Can't open table file for" << loadpath << "\n";
	}
    while (rec.SuckNextRecord (&f_schema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			 cerr << counter << "\n";
		}
		//if(page is full)
		curPage.Append(&rec);		   
    }
    curFile.AddPage(&curPage, curPageIndex);

}

//Each DBfile instance has a pointer to the current record in the file. 
//By default, this pointer is at the first record in the file, but it 
//can move in response to record retrievals.The following function 
//forces the pointer to correspond to the first record in the file
void DBFile::MoveFirst () {
	 curPageIndex = 0;
	 curFile.GetPage(&curPage, curPageIndex);
}

//In order to add records to the file, the following function is uesd. 
//In the case of the unordered heap file this function simply adds the 
//new record to the end of the file. This function should actually 
//consume addMe, so that after addMe has been put into the file, it 
//cannot be used again
void DBFile::Add (Record &rec) {
	curPage.Append(&rec);	
	rec.~Record ();//may be problem
}

//There are two functions that allow for record retrieval from a DBFile 
//instance. The first version simply gets the next record from the file 
//and returns it to user, where "next" is defined to be relative to the 
//current location of the pointer. After the function call returns, the 
//porinter into the file is incremented, the return value is zero if and 
//only if there is not a valid record returned from the function call
int DBFile::GetNext (Record &fetchme) {
	 
	 curFile.GetPage(&curPage, curPageIndex);
     if(curPage.GetFirst(&fetchme) != 0){
		 return 1;
	 }
	 else{
		 return 0;
     }	
}

//The next version also accepts a selection predicate(CNF). It returns 
//the next record in the file that is accepted by the selection predicte
//The literal record is used to check the selection predicate, and is 
//created when the parse tree for the CNF is processed
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
