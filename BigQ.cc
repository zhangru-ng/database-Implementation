#include "BigQ.h"


BigQ::BigQ (Pipe &i, Pipe &o, OrderMaker &sorder, int rl): in(i), out(o), sortorder(sorder), runlen(rl), workthread(){
	//create own worker thread for this BigQ
 	pthread_create (&workthread, NULL, &workerthread_wrapper, this);	  
}

BigQ::~BigQ () {
}
//C++ class member functions have a hidden this parameter passed in, 
//use a static class method (which has no this parameter) to bootstrap the class
void* BigQ::workerthread_wrapper (void* arg) {
    ((BigQ*)arg)->TPM_MergeSort();

}

//two phase multiway merge sort
void * BigQ::TPM_MergeSort(){//two phase multiway merge sort
	char filename[256]="dbfile/tempfile.bin";
	runsFile.Open(0, filename);
	vector<Run> runs;
	//First Phase of TPMMS
	FirstPhase(runs);
	//Second Phase of TPMMS
	SecondPhase(runs);
	runsFile.Close();
	out.ShutDown ();
	pthread_exit(NULL);
}

void BigQ::SortInRun(vector<Record> &oneRunRecords){
	Sorter runSorter(sortorder);
	sort(oneRunRecords.begin(), oneRunRecords.end(), runSorter);
}

//write one run to temporary file
void BigQ::WriteRunToFile(vector<Record> &oneRunRecords){	
	Page tempPage;
	Record tempRec;
	int tempIndex;

	if(runsFile.GetLength() == 0){
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page, will not 
	//write two oneRunRecords to the same page
	else if(runsFile.GetLength() > 0){
		tempIndex = runsFile.GetLength() - 1;
	}
	else{ //length less than 0, something wrong with current file
		cerr << "File length less than 0 in DBFile add\n";
		exit (1);
	}

	while (oneRunRecords.size() > 0) {//while there are records in this oneRunRecords
		tempRec = oneRunRecords.back();
		if( tempPage.Append(&tempRec) == 1){
			oneRunRecords.pop_back();
		}else{
			//if the page is full, create a new page
			runsFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempPage.Append(&tempRec);
			oneRunRecords.pop_back();
			tempIndex++;
		}
	}		 
	//there may be less than one page records in the last page 
	runsFile.AddPage(&tempPage, tempIndex);  
}

//First phase of TPMMS
void BigQ::FirstPhase(vector<Run> &runs){ 
	int curLen = 0;
	int curSize = 0;
	int runNum = 0;
	Record tempRec;
	vector<Record> oneRunRecords;
	
	while(in.Remove(&tempRec)){
		//check if a single record larger than a page
		if(tempRec.Size() > PAGE_SIZE - sizeof(int)){ 
			cerr << "record larger than a page in TPMMS first phase\n";
			exit(1);
		}

		//the sum of record size is larger than oneRunRecords size
		curSize += tempRec.Size();

		if(curSize > PAGE_SIZE - sizeof(int)){ //Page class initially allocate size of int to store curSizeInBytes 
			curLen++;			
			curSize = tempRec.Size();
		}
	
		//current length equal to oneRunRecords length, the current oneRunRecords is full
		if(curLen == runlen){
			//sort the current run
			SortInRun(oneRunRecords);
			//write the sorted run to temporary file
			WriteRunToFile(oneRunRecords);
			//add a new run to vector<Run> runs
			runs.push_back(Run(runNum, runNum * runlen, runlen, &runsFile));
			//clear the current run which has written to file
			oneRunRecords.clear();
			//clear current run counter
			curLen = 0;
			//run number plus 1
			runNum++;
		}
		//add the record to this run
		oneRunRecords.push_back(tempRec);
	}
	//the size of last oneRunRecords may be less than one run
	if(oneRunRecords.size() > 0){
		SortInRun(oneRunRecords);
		WriteRunToFile(oneRunRecords);
		//add as a run, the last page count as one page, current run length plus 1
		curLen++;
		runs.push_back(Run(runNum, runNum * runlen, curLen, &runsFile));
		runNum++;
	}
}

//Second phase of TPMMS
void BigQ::SecondPhase(vector<Run> &runs){	
	int minID = 0;
	Sorter sorter(sortorder);
	QueueMember tempQM;
	vector<Run>::iterator it;

	//priority queue of first record of each run
	priority_queue<QueueMember, vector<QueueMember>, Sorter>pqueue(sorter);

	//get the first record from each run
	for (it=runs.begin(); it!=runs.end(); ++it){
		tempQM.runID = it->GetRunID();
		it->GetNext(tempQM.rec);
		pqueue.push(tempQM);
	}

	while (! pqueue.empty()) {
		//get the minimal record
     	tempQM = pqueue.top();
     	//store the run ID of the smallest record
     	minID = tempQM.runID;
     	//insert the smallest record to output pipe
      	out.Insert(&tempQM.rec);
      	//pop the minimal record
      	pqueue.pop();
      	//if there are more record in the run which has just extracted the minimal record
     	if(runs[minID].GetNext(tempQM.rec)){
			tempQM.runID = minID;
			//push the next record of this run in queue
			pqueue.push(tempQM);
 		}
	}	
}

 Run::Run(int ID, int beg, int rl, File *rf){
 	runID = ID;
  	runBegIndex = beg;
  	runCurIndex = beg;
  	curRunLen = rl;
  	runsFile = rf;
  	runsFile->GetPage(&curPage, runBegIndex);
 }

//Copy constructer
 Run::Run(const Run &r){
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	runsFile->GetPage(&curPage, runCurIndex);
 }

//"=" operator
 Run::~Run(){ 	
 }

 Run& Run::operator = (const Run r){
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	runsFile->GetPage(&curPage, runCurIndex);
  	return *this;
 }

//get the ID of this run
int Run::GetRunID(){
	return runID;
}

//get the next record of this run 
int Run::GetNext(Record &curRec){
 	while(runCurIndex < runBegIndex + curRunLen){
 		if(curPage.GetFirst(&curRec)){
 			return 1;
 		}else{
 			if(++runCurIndex < runBegIndex + curRunLen){
 				curPage.EmptyItOut();
 				runsFile->GetPage(&curPage, runCurIndex); 
 			}else{
 				break;
 			}			
 		}
 	}
 	return 0;
 }

