#include "BigQ.h"


BigQ::BigQ (Pipe &i, Pipe &o, OrderMaker &sorder, int rl): in(i), out(o), sortorder(sorder), runlen(rl), runsFileName(NULL), runsFile(), runNum(0), workthread() {
	if (runlen > 0) {
		StartInternalThread();
 	} else {
		cerr << "ERROR in BigQ: run length should greater than 0!";
		exit(1);
	}
}

BigQ::~BigQ () {
	if (remove(runsFileName)) {
	 	cerr << "can't delete" << runsFileName;
	}
	free(runsFileName);
}

//two phase multiway merge sort
void *BigQ::InternalThreadEntry () {
// void * BigQ::TPM_MergeSort(){//two phase multiway merge sort
	char dir[80] = "dbfile/temp/BigQtemp_XXXXXX";
	mkstemp(dir);
	runsFileName = strdup(dir);
	runsFile.Open(0, runsFileName);
	vector<Run> runs;
	runs.reserve(200);
	// double start, end;
	// double cpu_time_used;
	// start = clock();		
	//First Phase of TPMMS
	FirstPhase(runs);
	//Second Phase of TPMMS
	SecondPhase(runs);
	// end = clock();
	// cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	// cout << "sort spent " << cpu_time_used << " seconds cpu time" << endl;
	out.ShutDown ();		
	runsFile.Close();
	//cout << "TPMMS spent totally " << cpu_time_used << " senconds cpu time" << endl;		
	pthread_exit(NULL);
}

void BigQ::SortInRun (vector<Record> &oneRunRecords) {
	ComparisonEngine comp;
	//use funny lambda function                                 [capture] (papameter) {body}
	sort(oneRunRecords.begin(), oneRunRecords.end(), [comp, this](const Record &r1, const Record &r2) { return comp.Compare (&r1, &r2, &sortorder) < 0; });
}

//write one run to temporary file, store the begin and length of current run
void BigQ::WriteRunToFile (vector<Record> &oneRunRecords, int &beg, int &len) {	
	Page tempPage;
	Record tempRec;
	int tempIndex;
	off_t length = runsFile.GetLength();
	if (0 == length) {
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page, will not 
	//write two oneRunRecords to the same page
	else if (length > 0) {
		tempIndex = length - 1;
	}
	else { //length less than 0, something wrong with current file
		cerr << "ERROR in BigQ tempFile: File length less than 0\n";
		exit (1);
	}
	beg = tempIndex;
	for (auto it = oneRunRecords.begin(); it != oneRunRecords.end(); ++it) {
		tempRec.Consume(&(*it));
		if (0 == tempPage.Append(&tempRec)) {
			//if the page is full, create a new page
			runsFile.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempPage.Append(&tempRec);
			tempIndex++;
		}
	}		 
	//there may be less than one page records in the last page 
	runsFile.AddPage(&tempPage, tempIndex);  
	//compute the length of this run
	len = tempIndex - beg + 1;
}

//First phase of TPMMS
void BigQ::FirstPhase (vector<Run> &runs) { 
	int curLen = 0;
	int curSize = sizeof(int);
	int beg, len;
	Record tempRec;
	vector<Record> oneRunRecords;
	oneRunRecords.reserve(800);
	while (in.Remove(&tempRec)) {
		//check if a single record larger than a page
		if (tempRec.Size() > PAGE_SIZE - sizeof(int)) { 
			cerr << "record larger than a page in TPMMS first phase\n";
			exit(1);
		}

		//the sum of record size is larger than oneRunRecords size
		curSize += tempRec.Size();

		if (curSize > PAGE_SIZE ) { //Page class initially allocate size of int to store curSizeInBytes 
			curLen++;			
			curSize = sizeof(int) + tempRec.Size();
		}
	
		//current length equal to oneRunRecords length, the current oneRunRecords is full
		if (curLen == runlen) {
			//sort the current run **sort exchange record positions, if run length greater than one page,
			//the new page of record after sorting may not fit in one page as before
			SortInRun(oneRunRecords);
			//write the sorted run to temporary file
			WriteRunToFile(oneRunRecords, beg, len);
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
	if (oneRunRecords.size() > 0) {
		SortInRun(oneRunRecords);
		WriteRunToFile(oneRunRecords, beg, len);
		//add as a run, the last page count as one page, current run length plus 1
		curLen++;
		runs.push_back(Run(runNum, beg, len, &runsFile));
		//increase run number
		runNum++;				
	}
}

//Second phase of TPMMS
void BigQ::SecondPhase (vector<Run> &runs) {	
	// double start, end;
	// double cpu_time_used;
	// start = clock();
	//Linear scan is much slower than priority queue when run number large
	if (runNum >= THRESHOLD) {
		PriorityQueue(runs);
	}
	// Linear scan is faster than priority queue when when run number small
	else {
		LinearScan(runs);
	}
	// end = clock();
	// cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
	// cout << "second phase spent " << cpu_time_used << " seconds cpu time" << endl;
	// cout << "PriorityQueue: spent " << cpu_time_used * 1000 <<" ms" << endl;
	// cout << "LinearScan: spent " << cpu_time_used * 1000 <<" ms" << endl;
}

void BigQ::LinearScan(vector<Run> &runs){
	ComparisonEngine comp;
	Sorter sorter(sortorder);
	ListMember tempLM;
	Record *minRec;
	list<ListMember> run_list;
	for (auto it=runs.begin(); it!=runs.end(); ++it){
		tempLM.myRun = &(*it);
		it->MoveFirst();
		it->GetNext(tempLM.rec);
		run_list.push_front(tempLM);
	}
	auto minIt = run_list.begin();
	while (0 == run_list.empty()) {
		minRec = &(minIt->rec);
		for (auto itl = run_list.begin(); itl != run_list.end(); ++itl){
			if (comp.Compare (minRec, &(itl->rec), &sortorder) > 0) {
				minRec = &(itl->rec);
				minIt = itl;
			}
		}
		out.Insert(minRec);
		if (false == (minIt->myRun)->GetNext(minIt->rec)) {
 			run_list.erase(minIt);
 			minIt = run_list.begin();
 		}
	}
}

void BigQ::PriorityQueue (vector<Run> &runs) {
	int minID = 0;	
	QueueMember tempQM;
	Sorter sorter(sortorder);
	//priority queue of first record of each run
	priority_queue<QueueMember, vector<QueueMember>, Sorter>pqueue(sorter);

	//for (it=runs.begin()+0; it!=runs.begin()+runNum; ++it){
	for (auto it=runs.begin(); it!=runs.end(); ++it) {
		tempQM.runID = it->GetRunID();
		it->MoveFirst();
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

Run::Run (int ID, int beg, int rl, File *rf) : runID(ID), runBegIndex(beg), runCurIndex(beg), curRunLen(rl), runsFile(rf) ,curPage(){ 
}

//Copy constructer
Run::Run (const Run &r) {
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	curPage = r.curPage;
}

//"=" operator
Run& Run::operator = (const Run &r) {
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	curPage = r.curPage;
  	return *this;
}

//get the ID of this run
int Run::GetRunID () {
	return runID;
}

void Run::MoveFirst () {
	runCurIndex = runBegIndex;
	if (runCurIndex + curRunLen - 1 <= runsFile->GetLength() - 2) {//if the file is not empty
		runsFile->GetPage(&curPage, runCurIndex);
	} else { //if the file is empty, cause "read past the end of the file"
		cerr << "ERROR: run exceeds file length";
		exit(1);
	}
}

//get the next record of this run 
int Run::GetNext (Record &curRec) {
 	while (1) {
 		if (curPage.GetFirst(&curRec)) {
 			return 1;
 		} else {
 			if (++runCurIndex < runBegIndex + curRunLen) {
 				curPage.EmptyItOut();
 				runsFile->GetPage(&curPage, runCurIndex); 
 			} else {
 				break;
 			}			
 		}
 	}
 	return 0;
 }

