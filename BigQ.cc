#include "BigQ.h"


BigQ::BigQ (Pipe &i, Pipe &o, OrderMaker &sorder, int rl): in(i), out(o), sortorder(sorder), runlen(rl),runNum(0), workthread(){
	//create own worker thread for this BigQ
 	pthread_create (&workthread, NULL, &workerthread_wrapper, this);	  
}

BigQ::~BigQ () {
	if(remove(runsFileName)){
		cerr << "can't delete" << runsFileName;
	}
}
//C++ class member functions have a hidden this parameter passed in, 
//use a static class method (which has no this parameter) to bootstrap the class
void* BigQ::workerthread_wrapper (void* arg) {
    ((BigQ*)arg)->TPM_MergeSort();

}

//two phase multiway merge sort
void * BigQ::TPM_MergeSort(){//two phase multiway merge sort
	runsFileName="dbfile/tempfile.bin";
	runsFile.Open(0, runsFileName);
	vector<Run> runs;
	//First Phase of TPMMS
	FirstPhase(runs);
	//Second Phase of TPMMS
	SecondPhase(runs);
	out.ShutDown ();	
	runsFile.Close();	
	pthread_exit(NULL);
}

void BigQ::SortInRun(vector<Record> &oneRunRecords){
	Sorter runSorter(sortorder);
	sort(oneRunRecords.begin(), oneRunRecords.end(), runSorter);
}

//write one run to temporary file, store the begin and length of current run
void BigQ::WriteRunToFile(vector<Record> &oneRunRecords, int &beg, int &len, File *rFile){	
	Page tempPage;
	Record tempRec;
	int tempIndex;
	int recIndex = 0;
	if(rFile->GetLength() == 0){
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page, will not 
	//write two oneRunRecords to the same page
	else if(rFile->GetLength() > 0){
		tempIndex = rFile->GetLength() - 1;
	}
	else{ //length less than 0, something wrong with current file
		cerr << "File length less than 0 in DBFile add\n";
		exit (1);
	}

	beg = tempIndex;

	while (recIndex < oneRunRecords.size()) {//while there are records in this oneRunRecords
		tempRec = oneRunRecords[recIndex];
		if( tempPage.Append(&tempRec) == 1){
			recIndex++;
		}else{
			//if the page is full, create a new page
			rFile->AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempPage.Append(&tempRec);
			recIndex++;
			tempIndex++;
		}
	}		 
	//there may be less than one page records in the last page 
	rFile->AddPage(&tempPage, tempIndex);  

	len = tempIndex - beg + 1;
}

//First phase of TPMMS
void BigQ::FirstPhase(vector<Run> &runs){ 
	int curLen = 0;
	int curSize = sizeof(int);
	//int runNum = 0;
	int beg, len;
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

		if(curSize > PAGE_SIZE ){ //Page class initially allocate size of int to store curSizeInBytes 
			curLen++;			
			curSize = sizeof(int) + tempRec.Size();
		}
	
		//current length equal to oneRunRecords length, the current oneRunRecords is full
		if(curLen == runlen){
			//sort the current run **sort exchange record positions, if run length greater than one page,
			//the new page of record after sorting may not fit in one page as before
			SortInRun(oneRunRecords);
			//write the sorted run to temporary file
			WriteRunToFile(oneRunRecords, beg, len, &runsFile);
			//add a new run to vector<Run> runs
			runs.push_back(Run(runNum, beg, len, &runsFile));
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
		WriteRunToFile(oneRunRecords, beg, len, &runsFile);
		//add as a run, the last page count as one page, current run length plus 1
		curLen++;
		runs.push_back(Run(runNum, beg, len, &runsFile));
		runNum++;
	}
}

//Second phase of TPMMS
void BigQ::SecondPhase(vector<Run> &runs){	
	int start1, start2, end1, end2;
	double cpu_time_used1, cpu_time_used2;
	//when the total run number is small, use std::priority_queue
	//if(runNum < 1000){
		start1 = clock(); 
		TwoPassMerge(runs);
		//PriorityQueue(runs);
		//LinearScan(runs);
		end1 = clock();
		cpu_time_used1 = ((double) (end1 - start1)) / CLOCKS_PER_SEC;
	//}
	//when the run number is big, use linear scan
	//else{
	/*	start2 = clock(); 
		LinearScan(runs);
		end2 = clock();
		cpu_time_used2 = ((double) (end2 - start2)) / CLOCKS_PER_SEC;*/
	//}	
	//may use multi pass merge for lager run number
	cout << "priority_queue:" << cpu_time_used1 << endl;
	//cout << "LinearScan:" << cpu_time_used2 << endl;
}

void BigQ::LinearScan(vector<Run> &runs){
	int count = 0;
	int runCount = 0;
	int minID = 0;
	Sorter sorter(sortorder);
	QueueMember tempQM;
	Record minRec;
	vector<Run>::iterator it;
	vector<QueueMember> qm;
	vector<QueueMember>::iterator itq;
	
	for (it=runs.begin(); it!=runs.end(); ++it){
		tempQM.runID = it->GetRunID();
		it->GetNext(tempQM.rec);
		qm.push_back(tempQM);
	}
	while( runCount < runNum ){
		for (itq=qm.begin(); itq!=qm.end(); ++itq){
			if(itq->runID >=0){
				minRec = itq->rec;
				minID = itq->runID;
				break;
			}			
		}	
		for (itq=qm.begin(); itq!=qm.end(); ++itq){
			if(itq->runID >=0){
				if(sorter(minRec, itq->rec) ){
					minRec = itq->rec;
					minID = itq->runID;
				}
			}			
		}	
		/*count++;
     	if(count % 100000 == 0){
     		cout << "Insert " << count << " records to output pipe" << endl;
     	}*/
		out.Insert(&minRec);
		if(qm[minID].runID >= 0 && runs[minID].GetNext(tempQM.rec)){
			tempQM.runID = minID;
			qm[minID] = tempQM;
 		}else{
 			qm[minID].runID = -1;
 			runCount++;
 		}	
	}
}

void BigQ::PriorityQueue(vector<Run> &runs){
	int count = 0;
	int minID = 0;
	Sorter sorter(sortorder);
	QueueMember tempQM;
	vector<Run>::iterator it;
	//priority queue of first record of each run
	priority_queue<QueueMember, vector<QueueMember>, Sorter>pqueue(sorter);

	//for (it=runs.begin()+0; it!=runs.begin()+runNum; ++it){
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
     /*count++;
     	if(count % 100000 == 0){
     		cout << "Insert " << count << " records to output pipe" << endl;
     	}*/
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

void BigQ::TwoPassMerge(vector<Run> &runs){
	char filename[256]="dbfile/mergefile.bin";
	int mergeBeg = 0;
	int mergeRunLen = 200;
	int mergeEnd = mergeBeg + mergeRunLen;
	int num = 0;
	int active_flag = 0;
	File mergeFile;
	vector<Run> newRuns;	
	mergeFile.Open(0, filename);
	while(mergeEnd <= runNum){
		MergeOnePass(runs, newRuns, mergeBeg, mergeEnd, num++, &mergeFile);
		mergeBeg = mergeEnd;
		mergeEnd = mergeBeg + mergeRunLen;
	}
	MergeOnePass(runs, newRuns, mergeBeg, runNum, num, &mergeFile);

	PriorityQueue(newRuns);
	mergeFile.Close();
	if(remove(filename)){
		cerr << "can't delete" << filename;
	}
}

void BigQ::MergeOnePass(vector<Run> &runs, vector<Run> &newRuns, int mergeBeg, int mergeEnd, int num, File *mergeFile){
	int count = 0;
	int minID = 0;
	int beg , len;
	QueueMember tempQM;
	Sorter sorter(sortorder);
	vector<Record> oneRunRecords;
	vector<Run>::iterator it;
	priority_queue<QueueMember, vector<QueueMember>, Sorter>pqueue(sorter);

	for (it=runs.begin()+mergeBeg; it!=runs.begin()+mergeEnd; ++it){
		tempQM.runID = it->GetRunID();
		it->GetNext(tempQM.rec);
		pqueue.push(tempQM);
	}
	while (! pqueue.empty()) {
		tempQM = pqueue.top();
       	minID = tempQM.runID;

		oneRunRecords.push_back(tempQM.rec);           	

       	pqueue.pop();
      	if(runs[minID].GetNext(tempQM.rec)){
			tempQM.runID = minID;
			pqueue.push(tempQM);
 		}
 	}
 	WriteRunToFile(oneRunRecords, beg, len, mergeFile);
 	newRuns.push_back(Run(num, beg, len, mergeFile));
 	cout << "##########beg" <<beg <<"\tlen"<<len<<endl;
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

