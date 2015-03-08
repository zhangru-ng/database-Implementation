
#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () : myOrder(), runLength(0), curMode(Read), filename(), bq(NULL), input(NULL), output(NULL), queryOrder(), isNewQuery(true) {	
}

SortedDBFile::~SortedDBFile () {
}

int SortedDBFile::Create (const char *f_path, fType f_type, void *startup) {
	//if no startup configuration passed in, can't create sorted file
	if(startup == NULL){
		cerr << "ERROR: Can't create sorted file, no startup configuration";
		exit(1);
	}
	string header = f_path;
	//generate header file name	
	header += ".header";
	//create the associated text file
	ofstream metafile( header.c_str() );
	if ( !metafile.is_open() ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	metafile << (int)f_type << endl;
	//read data from startup	
	SortInfo* sip = reinterpret_cast<SortInfo*> (startup);
	if(sip -> order == NULL){
		cerr << "ERROR: Can't create sorted file, startup configuration is wrong\n";
		return 0;
	}
	myOrder = *(sip -> order);
	runLength = sip -> runlen;
	if(myOrder.GetNumAtts() <= 0 || runLength <= 0){
		cerr << "ERROR: Can't create sorted file, startup configuration is wrong\n";
		return 0;
	}	
	//write data to associate file
	//ofstream operater << is overloaded for OrderMaker
	metafile << myOrder;
	metafile << runLength;
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
		cerr << "Can't open associated file for Sorted File: " << f_path << "\n";
		return 0;
	}	
	//read data from associate file
	int temp;
	metafile >> temp;//ignore the file type 
	//ifstream operater >> is overloaded for OrderMaker
	metafile >> myOrder;
	metafile >> runLength;
	metafile.close ();
	//someone may externally modify the header file and make attribute number or run length <= 0
	if(myOrder.GetNumAtts() <= 0 || runLength <= 0){
		cerr << "ERROR: Can't open sorted file, startup configuration is wrong in header file";
		return 0;
	}	
	curFile.Open (1, f_path);
	filename = f_path;
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, const char *loadpath) {
	FILE *tableFile = fopen (loadpath, "r");
	if(tableFile == NULL){
		cerr << "Can't open table file: " << loadpath << "\n";
	}
	//if DBFile's current mode is wrtie
	if(curMode == Read){
		initBigQ();
		curMode = Write;
		isNewQuery = true;
	}
	Record tempRec;
    while (tempRec.SuckNextRecord (&f_schema, tableFile) == 1) {
    	//insert record to input pipe
		input->Insert (&tempRec);
    }
    fclose(tableFile);
}

void SortedDBFile::Add (Record &rec) {
	//if DBFile's current mode is read
	if(curMode == Read){
		initBigQ();
		curMode = Write;
		isNewQuery = true;
	}
	//insert record to input pipe
	input->Insert (&rec);
}

void SortedDBFile::MoveFirst () {
	//if DBFile's current mode is wrtie, merge bigQ and sorted file
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
	//if DBFile's current mode is wrtie, merge bigQ and sorted file
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	curFile.Close ();
	if(bq != NULL){
		delete input;
		delete output;	
		delete bq;			
	}
	return 1;
}

int SortedDBFile::GetNext (Record &fetchme) {
	//if DBFile's current mode is wrtie, merge bigQ and sorted file
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	while(1){//if there is no "hole" in the file, while is not necessary
		 if( curPage.GetFirst(&fetchme)){//fetch record successfully
			return 1;
		 }
		 if(++curPageIndex <= curFile.GetLength() - 2){
			curPage.EmptyItOut();
			curFile.GetPage(&curPage, curPageIndex); 
			continue;
		 }
		break;
     }	
     return 0;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//if DBFile's current mode is wrtie, merge bigQ and sorted file
	if(curMode == Write){
		curMode = Read;
		MergeSortedParts();
	}
	//if the file is empty or fully scanned
	if(curPageIndex > curFile.GetLength() - 2){	
		return 0;	
	}	
	//in practice, the caller will never switches parameters without call MoveFirst or some kind of write
	//so only recompute queryOrder and BinarySearch after MoveFirst or some kind of write
	if(isNewQuery == false){
		return GetNextRecord(fetchme, cnf, literal);
	}
	OrderMaker searchOrder, dummy;
	//build search OrderMaker using the query CNF
	cnf.GetSortOrders(searchOrder, dummy);
	//array for converting query order to literal order
	int lit_order[MAX_ANDS];
	//generate the query OrderMaker using myOrder and SearchOrder
	MakeQueryOrder(searchOrder, lit_order);
	//if the query OrderMaker is not empty
	if(queryOrder.GetNumAtts() > 0){
		//use query OrderMaker to run a binary search on the file
		int matchIndex = BinarySearch(literal, fetchme, lit_order);
		//if no record that “equals” the literal record
		if(matchIndex == -1){
			//current page index out of file, indicate no record is accepted in whole file
			curPageIndex = curFile.GetLength() - 1;//may cause read out of bound 
			return 0;
		}else{
			curPageIndex = matchIndex;
			return FindAcceptedRecord(fetchme, literal, cnf, lit_order);
		}
	}else{
		//has no useful sorting attributes, search from first current record
		return GetNextRecord(fetchme, cnf, literal);
	}	

}

//initial the internal BigQ of this file
void SortedDBFile::initBigQ(){
	//there is no way to activate pipes after they are shut down, delete old pipes&BigQ, allocte new pipes when next write occur
	delete input;
	delete output;	
	delete bq;	
	input = new (std::nothrow) Pipe (buffsz);
	if (input == NULL){
		cerr << "ERROR : Not enough memory for input buffer of " << filename;
		exit(1);
	}
	output = new (std::nothrow) Pipe (buffsz);
	if (output == NULL){
		cerr << "ERROR : Not enough memory for output buffer of " << filename;
		exit(1);
	}
	bq = new (std::nothrow) BigQ (*input, *output, myOrder, runLength);
	if (bq == NULL){
		cerr << "ERROR : Not enough memory for bigQ of " << filename;
		exit(1);
	}
}	

//merge the file's internal BigQ with its other sorted data when change from write to read or close the file
void SortedDBFile::MergeSortedParts() {
	Record tempRec;
	Page tempPage;
	File tempFile;
	int tempIndex = 0;
	string tempFilename = filename;
	tempFilename += ".temp";
	//open the temporary file to store merged records
	tempFile.Open(0, tempFilename.c_str());	
	//shut down input pipe
	input->ShutDown ();	
	//Merge records in sorted file and bigQ until one of them reaches the end
	MergeRecord(tempPage, tempFile, tempIndex);
	//add the remaining records in the old DBFile to new DBFile	
	while(GetNext(tempRec)){
		AddRecord(tempRec, tempPage, tempFile, tempIndex);
	}
	//add the remaining records in the output pipe to new DBFile
	while(output->Remove(&tempRec)){
		AddRecord(tempRec, tempPage, tempFile, tempIndex);
	}
	//last page may not full to trigger to writing to file
	tempFile.AddPage(&tempPage, tempIndex);  	
	//shut down output pipe
	//output->ShutDown();  //BigQ will shut down output pipe
	//close new merged file and old sorted file
	tempFile.Close();
	curFile.Close();
	//remove the old DBFile
	remove(filename.c_str());
	//rename the new merged DBFile to old name
	rename(tempFilename.c_str(), filename.c_str());
	//open the new merged DBFile
	curFile.Open(1, filename.c_str());	
}

void SortedDBFile::AddRecord(Record &tempRec, Page &tempPage, File &tempFile, int &tempIndex){
	if( tempPage.Append(&tempRec) == 0){
		//if the page is full, create a new page
		tempFile.AddPage(&tempPage, tempIndex);  
		tempPage.EmptyItOut();
		tempIndex++;
		tempPage.Append(&tempRec);
	}	
}

void SortedDBFile::MergeRecord(Page &tempPage, File &tempFile, int &tempIndex){
	Record tempRec1, tempRec2, minRec;
	ComparisonEngine comp;	
	MoveFirst();	
	Schema mySchema("catalog","customer");
	//true if sorted file has at least one record
	bool fileHasRecord = GetNext(tempRec1);
	//true if BigQ has at least one record
	bool bigQHasRecord = output->Remove(&tempRec2);
	//if both the sorted DBFile and output pipe have at least one record
	if( fileHasRecord && bigQHasRecord ){
		while(1){
			//if the record in old DBFile is smaller
			if(comp.Compare (&tempRec1,&tempRec2, &myOrder) < 0){
				minRec.Consume(&tempRec1);
				AddRecord(minRec, tempPage, tempFile, tempIndex);
				//get next record in DBFile for next comparion, if no record left then break
				if(GetNext(tempRec1) == 0){				
					AddRecord(tempRec2, tempPage, tempFile, tempIndex);
					break;
				}
			}
			//if the record in output pipe is smaller
			else{
				minRec.Consume(&tempRec2);
				AddRecord(minRec, tempPage, tempFile, tempIndex);
				//get next record in output pipe for next comparion, if no record left then break
				if(output->Remove(&tempRec2) == 0){					
					AddRecord(tempRec1, tempPage, tempFile, tempIndex);
					break;
				}
			}
		}
	}else if(fileHasRecord){
		//if sorted file has record and bigQ is empty
		AddRecord(tempRec1, tempPage, tempFile, tempIndex);
	}else if(bigQHasRecord){
		//if bigQ has record and sorted file is empty
		AddRecord(tempRec2, tempPage, tempFile, tempIndex);
	}
}

//simple GetNext sub-function used when there's no attribute in query OrderMaker
int SortedDBFile::GetNextRecord(Record &fetchme, CNF &cnf, Record &literal){
	ComparisonEngine comp;
	while( GetNext(fetchme) ){//fetch record successfully
		if(comp.Compare(&fetchme, &literal, &cnf)){
			return 1;
		}				
	}	
	return 0;
}

//use the file OrderMaker myOrder and SearchOrder derive from input CNF to built query OrderMaker
void SortedDBFile::MakeQueryOrder(OrderMaker &searchOrder, int *litOrder){
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
					litOrder[i] = j;
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
	//query OrderMaker has updated, set isNewQuery to false
	isNewQuery = false;
}

//use in conjunction with query OrderMaker to speed up GetNext
//if find the match record, return the match page index
//otherwise return -1
int SortedDBFile::BinarySearch(Record &literal, Record &outRec, int *litOrder) {
	if(GetNext(outRec) == 0){
		return -1;
	}
	ComparisonEngine comp;
	int compare_result = comp.Compare(&outRec, &literal, &queryOrder, litOrder);
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
  
  	bool scanNextPage = false;
	int matchIndex;
	//initial the min and max page index	
	int minIndex = curPageIndex;
	int maxIndex = curFile.GetLength() - 2;
  	while (minIndex < maxIndex){
  		//compute the middle page index
    	int midIndex = (minIndex + maxIndex) / 2;
      	//read first record of middle page
    	curFile.GetPage(&curPage, midIndex);
    	curPage.GetFirst(&outRec);
    	//if the record is "<" literal
      	if (comp.Compare(&outRec, &literal, &queryOrder, litOrder) < 0){
      		minIndex = midIndex + 1;
      	}
      	//if the record is ">=" literal
      	else{
      	//if the record is "=" literal, there maybe other match records in preivous pages, 
      	//keep on search the preivous pages. When the binary search terminate, scan one more 
      	//page to find match record
      		if(comp.Compare(&outRec, &literal, &queryOrder, litOrder) == 0){
      			//set scanNextPage to true to mark a match record is found, 
      			scanNextPage = true;
      		} 
      		maxIndex = midIndex;
       	}
    }
    //when the binary search terminate, minIndex = maxIndex, if the match record exists, its Page index is midIndex
    //which is minIndex - 1 (or maxIndex - 1). However, there is an exception when minIndex reach end of file
    matchIndex = minIndex - 1;
   	//deferred test for equality to ensure find the first match record
  	//if find the match record, return the match page index
  	curFile.GetPage(&curPage, matchIndex);    
  	while(curPage.GetFirst(&outRec)){
  		if ((comp.Compare(&outRec, &literal, &queryOrder, litOrder) == 0)){
  			return matchIndex;
  		}
  	}
  	//if minIndex reach the end of file, can not determine whether the match record in second last page or not, 
  	//or if scanNextPage is true, scan one more page to ensure no match record is lost
  	if( scanNextPage == true || minIndex == curFile.GetLength() - 2 ){
  		curFile.GetPage(&curPage, ++matchIndex);    
  		while(curPage.GetFirst(&outRec)){
  			if ((comp.Compare(&outRec, &literal, &queryOrder, litOrder) == 0)){
  				return matchIndex;
  			}
  		}
  	}
	//otherwise return -1
	return -1;
}

//after the binary search locate match record, examine record one-by-one to find accepted record
//return 1 if find a accepted record, otherwise return 0 
int SortedDBFile::FindAcceptedRecord(Record &fetchme, Record &literal, CNF &cnf, int *litOrder){
	ComparisonEngine comp;
	//if the query CNF accept the record, return 1
	if(comp.Compare(&fetchme, &literal, &cnf)){
		return 1;
	}else{
		//scan record one-by-one
		while( GetNext(fetchme) ){//fetch record successfully
			//if query OrderMaker accept the record
			if(comp.Compare(&fetchme, &literal, &queryOrder, litOrder) == 0){
				//if query CNF  accept the record, return 1
				if(comp.Compare(&fetchme, &literal, &cnf)){
					return 1;
				}				
			}
			//if query OrderMaker dosen't accept the record, return 0
			else{
				return 0;
			}
		}	
		return 0;
	}
}


