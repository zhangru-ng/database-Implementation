
#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () : myOrder(), runLength(0), curMode(Read), input(NULL), output(NULL), bq(NULL), isNewQuery(true) {	

}

SortedDBFile::~SortedDBFile () {
	
}

int SortedDBFile::Create (const char *f_path, fType f_type, void *startup) {
	string header = f_path;
	//generate header file name	
	header += ".header";
	//create the associated text file
	ofstream metafile( header.c_str() );
	if ( !metafile.is_open() ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	metafile << "sorted" << endl;
	//read data from startup
	SortInfo* sip = reinterpret_cast<SortInfo*> (startup);
	myOrder = *(sip -> order);
	runLength = sip -> runlen;
	//write data to associate file
	metafile.write (reinterpret_cast<const char *> (&myOrder), sizeof (OrderMaker));
	metafile << runLength << endl;
	metafile.close();

	//create the binary DBfile
	curFile.Open (0, f_path);
	filename = f_path;
	return 1;
}

int SortedDBFile::Open (const char *f_path) {
	string header = f_path;
	//generate header file name	
	header += ".header";
	//open the exist associate file
	ifstream metafile( header.c_str() );
	if ( !metafile.is_open () ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}	
	//read data from associate file
	metafile.seekg (7, metafile.beg);  //ignore the first 6 bytes which stores file type
	metafile.read (reinterpret_cast<char *> (&myOrder), sizeof (OrderMaker));
	metafile >> runLength;

	metafile.close ();
	curFile.Open (1, f_path);
	filename = f_path;
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, const char *loadpath) {
	Record tempRec;
	FILE *tableFile = fopen (loadpath, "r");

	if(tableFile == NULL){
		cerr << "Can't open table file: " << loadpath << "\n";
	}
	//if DBFile's current mode is wrtie
	if(curMode == Read){
		if( bq == NULL){
			input = new Pipe (buffsz);
			output = new Pipe (buffsz);
			bq = new BigQ (*input, *output, myOrder, runLength);
		}		
		curMode = Write;
		isNewQuery = true;
	}
	
    while (tempRec.SuckNextRecord (&f_schema, tableFile) == 1) {
    	//insert record to input pipe
		input->Insert (&tempRec);
    }
    fclose(tableFile);
}

void SortedDBFile::Add (Record &rec) {
	//if DBFile's current mode is read
	if(curMode == Read){
		if( bq == NULL){
			input = new Pipe (buffsz);
			output = new Pipe (buffsz);
			bq = new BigQ (*input, *output, myOrder, runLength);
		}		
		curMode = Write;
		isNewQuery = true;
	}
	//insert record to input pipe
	input->Insert (&rec);
}

void SortedDBFile::MoveFirst () {
	//if DBFile's current mode is wrtie
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	isNewQuery = true;
	curPageIndex = 0;
	if(curFile.GetLength() > 0){//if the file is not empty
		curFile.GetPage(&curPage, curPageIndex);
	}else{ //if the file is empty, cause "read past the end of the file"
		curPage.EmptyItOut();
	}
}

int SortedDBFile::Close () {
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	curFile.Close ();
	if(bq != NULL){
		delete bq;
		delete input;
		delete output;		
	}
	return 1;
}

int SortedDBFile::GetNext (Record &fetchme) {
	if(curMode == Write){
		MergeSortedParts();
	}
	while(curPageIndex <= curFile.GetLength() - 2 ){	
		while( curPage.GetFirst(&fetchme) ){//fetch record successfully
			return 1;				
		 }	
		 if(curPageIndex < curFile.GetLength() - 2 ){
			curPage.EmptyItOut();
			curFile.GetPage(&curPage, ++curPageIndex); 
		 }else{
			break;
		 }
	}
	return 0;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//if DBFile's current mode is wrtie
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	//if the file is empty
	if(curFile.GetLength() == 0 ){	
		cerr << "In Sorted File GetNext, File is empty";
		return 0;	
	}	
	if(curPageIndex > curFile.GetLength() - 2){	
		cerr << "In Sorted File GetNext, Page index exceed file length";
		return 0;	
	}	
	//in practice, the caller will never switches parameters without call MoveFirst or some kind of write
	//so only recompute queryOrder and BinarySearch after MoveFirst or some kind of write
	if(isNewQuery = false){
		if(queryOrder.GetNumAtts() > 0){
			if(GetNext(fetchme)){
				return FindAcceptedRecord(fetchme, literal, cnf);
			}else{
				return 0;
			}			
		}else{
			return GetNextRecord(fetchme, cnf, literal);
		}
	}
	OrderMaker searchOrder, dummy;
	//build search OrderMaker using the query CNF
	cnf.GetSortOrders (searchOrder, dummy);
	//generate the query OrderMaker using myOrder and SearchOrder
	MakeQueryOrder(searchOrder);
	//if the query OrderMaker is not empty
	if(queryOrder.GetNumAtts() > 0){
		//use query OrderMaker to run a binary search on the file
		int matchIndex = BinarySearch(literal, fetchme);
		//if no record that “equals” the literal record
		if(matchIndex == -1){
			curPageIndex = curFile.GetLength() - 1;//may cause read out of bound 
			return 0;
		}else{
			curPageIndex = matchIndex;
			return FindAcceptedRecord(fetchme, literal, cnf);
		}
	}else{
		//has no useful sorting attributes, search from first current record
		return GetNextRecord(fetchme, cnf, literal);
	}	
}

//merge the file's internal BigQ with its other sorted data when change from write to read or close the file
void SortedDBFile::MergeSortedParts() {
	File tempFile;
	Page tempPage;
	Record tempRec1, tempRec2;
	Record minRec;

	string tempFilename = filename;
	tempFilename += ".temp";
	tempFile.Open(0, tempFilename.c_str());
	ComparisonEngine comp;
	int tempIndex = 0;
	input->ShutDown ();
	

	MoveFirst();	
	if( GetNext(tempRec1) && output->Remove(&tempRec2) ){
		while(1){
			if(comp.Compare (&tempRec1,&tempRec2, &myOrder) <= 0){
				minRec = tempRec1;
				if(!GetNext(tempRec1)){
					break;
				}
			}else{
				minRec = tempRec2;
				if(!output->Remove(&tempRec2)){
					break;
				}
			}
			if( tempPage.Append(&minRec) == 0){
				//if the page is full, create a new page
				tempFile.AddPage(&tempPage, tempIndex);  
				tempPage.EmptyItOut();
				tempIndex++;
				tempPage.Append(&minRec);
			}	
		}
	}
	
	while(GetNext(tempRec1)){
		if( tempPage.Append(&tempRec1) == 0){
			//if the page is full, create a new page
			tempFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempIndex++;
			tempPage.Append(&tempRec1);
		}	
	}
	while(output->Remove(&tempRec2)){
		if( tempPage.Append(&tempRec2) == 0){
			//if the page is full, create a new page
			tempFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempIndex++;
			tempPage.Append(&tempRec2);
		}	
	}
	tempFile.AddPage(&tempPage, tempIndex);  
	output->ShutDown();
	tempFile.Close();
	curFile.Close();
	remove(filename.c_str());
	rename(tempFilename.c_str(), filename.c_str());
	curFile.Open(1, filename.c_str());	
}

//simple GetNext sub-function used when there's no attribute in query OrderMaker
int SortedDBFile::GetNextRecord(Record &fetchme, CNF &cnf, Record &literal){
	ComparisonEngine comp;
	while(curPageIndex <= curFile.GetLength() - 2 ){	
		while( curPage.GetFirst(&fetchme) ){//fetch record successfully
			if(comp.Compare(&fetchme, &literal, &cnf)){
				return 1;
			}				
		 }	
		 if(curPageIndex < curFile.GetLength() - 2 ){
			curPage.EmptyItOut();
			curFile.GetPage(&curPage, ++curPageIndex); 
		 }else{
			break;
		 }
	}
	return 0;
}

//use the file OrderMaker myOrder and SearchOrder derive from input CNF to built query OrderMaker
void SortedDBFile::MakeQueryOrder(OrderMaker &searchOrder){
	//Get attributes used to sort the file
	int *sortedFileAtts = myOrder.GetAtts();
	//Get attributes type used to sort the file
	Type *sortedFileType =  myOrder.GetType();
	//Get number of attributes used to sort the file
	int sortedFileNumAtts = myOrder.GetNumAtts();

	//Get attributes used to in the search CNF
	int *searchAtts = searchOrder.GetAtts();	
	//Get number of attributes used to in the search CNF
	int searchNumAtts = searchOrder.GetNumAtts();

	int i , j;
	//clear the old query Ordermaker
	queryOrder.Clear();
	//squential scan the attributes in myOrder(scan order is crucial)
	for( i = 0; i < sortedFileNumAtts; i++){
		for( j = 0; j < searchNumAtts; j++){
			//if the attribute is present in any of the subexpressions in the search CNF 
			if( searchAtts[j] == sortedFileAtts[i] ){
				//add the attribute to the query OrderMaker
				if( queryOrder.Add(sortedFileAtts[i], sortedFileType[i]) ){
					break;
				}				
			}
		}
		//if any attribute in sorted file OrderMaker is not present in the search CNF  
		//stop building up your query OrderMaker
		if( j == searchNumAtts ){
			break;
		}	
	}
	isNewQuery = false;
}

//use in conjunction with query OrderMaker to speed up GetNext
int SortedDBFile::BinarySearch(Record &literal, Record &outRec)
{
	curPage.GetFirst(&outRec);
	ComparisonEngine comp;
	int compare_result = comp.Compare(&outRec, &literal, &queryOrder);
	//if current record "equal to" the literal record, return current page index
    if(compare_result == 0){
    	return curPageIndex;
    }
    //if current record "greater than" the literal record, all of the rest records 
    //in the file are greater than literal 
    else if(compare_result > 0){
    	return -1;
    }
    //if current record "less than" the literal record, run the binary search

	int minIndex = curPageIndex;
	int maxIndex = curFile.GetLength() - 2;

  	while (minIndex < maxIndex)
    {
    	int midIndex = (minIndex + maxIndex) / 2;
  
    	curFile.GetPage(&curPage, midIndex);
    	curPage.GetFirst(&outRec);
      	if (comp.Compare(&outRec, &literal, &queryOrder) < 0){
        	minIndex = midIndex + 1;//maybe don't plus 1
      	}else{
       		maxIndex = midIndex;
       	}
    }
  // At exit of while:
  //   if file is empty, then maxIndex < minIndex
  //   otherwise maxIndex == minIndex
 
  // deferred test for equality
	if ((maxIndex == minIndex) && (comp.Compare(&outRec, &literal, &queryOrder) == 0)){
		return minIndex;
	}else{
		return -1;
	}    	
}

//after the binary search locate match record, examine record one-by-one to find accepted record
int SortedDBFile::FindAcceptedRecord(Record &fetchme, Record &literal, CNF &cnf){
	ComparisonEngine comp;
	if(comp.Compare(&fetchme, &literal, &cnf)){
		return 1;
	}else{
		while(curPageIndex <= curFile.GetLength() - 2 ){	
			while( curPage.GetFirst(&fetchme) ){//fetch record successfully
				if(comp.Compare(&fetchme, &literal, &queryOrder) == 0){
					if(comp.Compare(&fetchme, &literal, &cnf)){
						return 1;
					}				
				}else{
					return 0;
				}
			}	
			if(curPageIndex < curFile.GetLength() - 2 ){
				curPage.EmptyItOut();
				curFile.GetPage(&curPage, ++curPageIndex); 
		 	}else{
				break;
		 	}
		}
		return 0;
	}
}


