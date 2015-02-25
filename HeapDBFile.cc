#include "HeapDBFile.h"

HeapDBFile::HeapDBFile () {

}

HeapDBFile::~HeapDBFile () {

}


//The following function is used to actually create the file. The first
//parameter is a text string that tells where the binary data is 
//physically to be located. If there is meta-data(type of file, etc),
//store in an associated text file(.header). The second parameter is an
//enumeration with three possible values: heap, sorted and tree. The 
//return value is a one on success and a zero on failure
int HeapDBFile::Create (char *f_path, fType f_type, void *startup) {
	string header = f_path;
	
	//create the binary DBfile
	curFile.Open (0, f_path);
	
	//create the associated text file
	header += ".header";
	
	ofstream metafile(header.c_str());
	if (!metafile.is_open()){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	metafile << "heap" << endl;
	metafile.close();	
	return 1;
}



//This function assumes that the DBFile already exists and has previous 
//been created and then closed. The one parameter to this function is 
//simply the physical location fo the file.The return value is a one on 
//success and a zero on failure(open auxiliary text file at startup)
int HeapDBFile::Open (char *f_path) {
	//open the exist DBfile
	curFile.Open (1, f_path);
	return 1;
}

//Simply close the file. The return value is a one on success and a zero 
//on failure
int HeapDBFile::Close () {
	off_t f_flag = curFile.Close ();
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
void HeapDBFile::Load (Schema &f_schema, char *loadpath) {
	FILE *tableFile = fopen (loadpath, "r");
	Record tempRec;
	Page tempPage;
	int tempIndex = 0;
	if(tableFile == NULL){
		cerr << "Can't open table file for" << loadpath << "\n";
	}
     while (tempRec.SuckNextRecord (&f_schema, tableFile) == 1) {
		if( tempPage.Append(&tempRec) == 0){
			//if the page is full, create a new page
			curFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			if( tempPage.Append(&tempRec) == 0 ){ //if fail again, record larger than page
				cerr << "Can't load " << loadpath << ", a record larger than page" << "\n";
				exit(1);
			}
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
void HeapDBFile::MoveFirst () {
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
void HeapDBFile::Add (Record &addMe) {
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
		cerr << "File length less than 0 in DBFile add";
		exit (1);
	}
	if( tempPage.Append(&addMe) ){//if the page is not full 
		curFile.AddPage(&tempPage, tempIndex);  
	}else{ //Page is full or record larger than page
		//clean the page and add the record that failed before
		tempPage.EmptyItOut();		
		if( !tempPage.Append(&addMe) ){ 
			cerr << "This record is larger than a DBFile page";
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
int HeapDBFile::GetNext (Record &fetchme) {	 //first time call this function,
										//MoveFirst() is needed 
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
int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {//first time call this function,
																//MoveFirst() is needed 
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
