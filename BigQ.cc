#include "BigQ.h"


BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
 	pthread_t workthread;
	pthread_create (&workthread, NULL, TwoPhaseMergeSort, (void *)&input);	
    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}

//two phase multiway merge sort
void PM_MergeSort(){//two phase multiway merge sort
	runsFile.Open(0, filename);
	FirstPhase();
	SecondPhase();
}

void SortInRun(vector<Record> &run, OrderMaker &sortorder){
	Sorter runSorter(sortorder);
	sort(run.begin(), run.end(), runSorter);
}

void WriteRunToFile(vector<Record> &run){	
	Page tempPage;
	Record tempRec;
	int tempIndex;
	runsFile.Open(1, filename);
	if(runsFile.GetLength() == 0){
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page, will not 
	//write two run to the same page
	else if(runsFile.GetLength() > 0){
		tempIndex = curFile.GetLength() - 1;
		runsFile.GetPage(&tempPage, tempIndex);		
	}
	else{ //length less than 0, something wrong with current file
		cerr << "File length less than 0 in DBFile add\n";
		exit (1);
	}

	while (run.size() > 0) {//while there are records in this run
		tempRec = run.back();
		if( tempPage.Append(tempRec) == 1){
			run.pop_back();
		}else{
			//if the page is full, create a new page
			runsFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempPage.Append(tempRec);
			run.pop_back();
			tempIndex++;
		}
	}		 

}

//First phase of TPMMS
void FirstPhase(Pipe &in, OrderMaker &sortorder, int runlen){ 
	int curLen = 0;
	int curSize = 0;
	int runNum = 0;
	Record tempRec;

	vector<Record> run;

	while(in.Remove(&tempRec)){
		//check if a single record larger than a page
		if(tempRec.Size() > PAGE_SIZE){ 
			cerr << "record larger than a page in TPMMS first phase\n";
			exit(1);
		}

		//the sum of record size is larger than run size
		curSize += tempRec.Size();
		if(curSize > PAGE_SIZE){ 
			curLen++;			
			curSize = tempRec.Size();
		}
	
		//current length greater than run length, the current run is full
		if(curLen > runlen){
			SortInRun(run, sortorder);
			WriteRunToFile(run);
			run.clear();
			curLen = 0;
			runNum++;
		}
		run.push_back(tempRec);
	}
	//the size of last run may be less than one page
	if(run.size() > 0){
		SortInRun(run, sortorder);
		WriteRunToFile(run);
	}
}

//Second phase of TPMMS
void SecondPhase(Pipe &out, OrderMaker &sortorder){

}
