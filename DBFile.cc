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

DBFile::DBFile () {
	curPageIndex = 0;
}

//convert enum to string to wrtie into a txt file
void enumToString(fType f_type, char *type){
	switch(f_type){
		case heap:
			strcpy(type, "heap");
			break;
		case sorted:
			strcpy(type, "sorted");
			break;
		case tree:
			strcpy(type, "tree");
			break;
	}
}

void GetHeaderFilePath(char *s, const char *f_path){
	char *temp = NULL;
	strcpy(s, f_path);
	if( (temp = strstr(s,".bin")) == NULL){
		strcpy(s ,"header");
	}else{
		strcpy(temp ,".header\0");
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
	GetHeaderFilePath(s, f_path);

	if(	(fp= fopen (s,"w")) == NULL){
		cerr << "Can't create associated file for" << f_path << "\n";
		return 0;
	}
	enumToString(f_type, type);
	fprintf(fp, "%s", type);
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
	GetHeaderFilePath(s, f_path);
	
	if(	(fp= fopen (s,"r")) == NULL){
		cerr << "Can't open associated file for" << f_path << "\n";
		return 0;
	}
	fscanf(fp,"%s",type);
	/***********************deal with different type of file
	 * switch(type){
	 * case "heap": break;
	 * case "sorted": break;
	 * case "tree": break;
	 * *********************/ 
	fclose(fp);	
	return 1;
}

//Simply close the file. The return value is a one on success and a zero 
//on failure
int DBFile::Close () {
	off_t f_flag;
	f_flag = curFile.Close ();
	if(f_flag < 0){
		return 0;
	}else{
		return 1;
	}
	
}

//This function bulk the DBFile instance from a text file, appending new
//data to it using the SuckNextRecord function from Record.h. 
//The character string passed to Load is the name of the data file to 
//bulk load
void DBFile::Load (Schema &f_schema, char *loadpath) {
	FILE *tableFile = fopen (loadpath, "r");
	Record tempRec;
	Page tempPage;
	int tempIndex = 0, counter = 0;
	if(tableFile == NULL){
		cerr << "Can't open table file for" << loadpath << "\n";
	}
     while (tempRec.SuckNextRecord (&f_schema, tableFile) == 1) {
		counter++;
		/*if (counter % 10000 == 0) {
			 cerr << counter << "\n";	//output for debug
		}*/
		if( tempPage.Append(&tempRec) == 0){
			//if the page is full, create a new page
			curFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			//again add the record that failed because page is full
			tempPage.Append(&tempRec);
			tempIndex++;
		}		   
    }
    //the records don't fill a full page, add the page directly
    curFile.AddPage(&tempPage, tempIndex);
}

//Each DBfile instance has a pointer to the current record in the file. 
//By default, this pointer is at the first record in the file, but it 
//can move in response to record retrievals.The following function 
//forces the pointer to correspond to the first record in the file
void DBFile::MoveFirst () {
	curPageIndex = 0;
	if(curFile.GetLength() > 0){//if the file is not empty
		curFile.GetPage(&curPage, curPageIndex);
	}else{ //if the file is empty, cause "read past the end of the file"
		curPage.EmptyItOut();
	}
}

//In order to add records to the file, the following function is uesd. 
//In the case of the unordered heap file this function simply adds the 
//new record to the end of the file. This function should actually 
//consume addMe, so that after addMe has been put into the file, it 
//cannot be used again
void DBFile::Add (Record &addMe) {
	Page tempPage;
	int tempIndex;	 
	//length is 0 for empty file, GetPage(&tempPage, 0) will lead to 
	//read over bound of file	
	if(curFile.GetLength() == 0){
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page
	else if(curFile.GetLength() > 0){
		tempIndex = curFile.GetLength() - 2;
		curFile.GetPage(&tempPage, tempIndex);		
	}
	else{ //length less than 0, something wrong with current file
		cerr << "File length less than 0 in DBFile add\n";
		exit (1);
	}
	if( tempPage.Append(&addMe) ){//if the page is not full
		curFile.AddPage(&tempPage, tempIndex);  
	}else{
		tempPage.EmptyItOut();
		//again add the record that failed because page is full
		if( !tempPage.Append(&addMe) ){
			cerr << "Can't add record to DBFile\n";
			exit (1);
		}
		tempIndex++;		
		curFile.AddPage(&tempPage, tempIndex);
	}	
}

//There are two functions that allow for record retrieval from a DBFile 
//instance. The first version simply gets the next record from the file 
//and returns it to user, where "next" is defined to be relative to the 
//current location of the pointer. After the function call returns, the 
//porinter into the file is incremented, the return value is zero if and 
//only if there is not a valid record returned from the function call
int DBFile::GetNext (Record &fetchme) {	 //first time call this function,
										//MoveFirst is needed 
		while(curPageIndex + 1 < curFile.GetLength()){
		 //GetFirst consume fetchme in curPage
		 if( curPage.GetFirst(&fetchme)){//fetch record successfully
			return 1;
		 }
		 if(curPageIndex + 2 < curFile.GetLength()){
			curPage.EmptyItOut();
			curFile.GetPage(&curPage, curPageIndex+1); 
			curPageIndex++;
		 }else{
			break;
		 }
     }	
     return 0;
}

//The next version also accepts a selection predicate(CNF). It returns 
//the next record in the file that is accepted by the selection predicte
//The literal record is used to check the selection predicate, and is 
//created when the parse tree for the CNF is processed
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {//first time call this function,
																//MoveFirst is needed 
	ComparisonEngine comp;
	while(curPageIndex + 1 < curFile.GetLength()){	
		while( curPage.GetFirst(&fetchme) ){//fetch record successfully
			if(comp.Compare(&fetchme, &literal, &cnf)){
				return 1;
			}				
		 }	
		 if(curPageIndex + 2 < curFile.GetLength()){
			curPage.EmptyItOut();
			curFile.GetPage(&curPage, curPageIndex+1); 
			curPageIndex++;
		 }else{
			break;
		 }
	}
	return 0;
}

