#include "BigQ.h"
#include <utility>

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
	char dir[80] = "dbfile/temp/BigQtemp_XXXXXX";
	//generate random file name, replace XXXXXX with random string
	mkstemp(dir);
	runsFileName = strdup(dir);
	runsFile.Open(0, runsFileName);
	//vector to store run information
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
			//add a new run to vector<Run> runs, use move to avoid copy
			runs.push_back(move(Run(runNum, beg, len, &runsFile)));
			//clear the current run which has written to file
			oneRunRecords.clear();
			//clear current run counter
			curLen = 0;
			//run number plus 1
			runNum++;			
		}
		//add the record to this run,  use move to avoid copy
		oneRunRecords.push_back(move(tempRec));
	}   
	//the size of last oneRunRecords may be less than one run
	if (oneRunRecords.size() > 0) {
		SortInRun(oneRunRecords);
		WriteRunToFile(oneRunRecords, beg, len);
		//add as a run, the last page count as one page, current run length plus 1
		curLen++;
		//add a new run to vector<Run> runs, use move to avoid copy
		runs.push_back(move(Run(runNum, beg, len, &runsFile)));
		//increase run number
		runNum++;				
	}
}

//Second phase of TPMMS
void BigQ::SecondPhase (vector<Run> &runs) {	
	// double start, end;
	// double cpu_time_used;
	// start = clock();
	// Linear scan is much slower than priority queue when run number large
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
	// ofstream metafile("time.txt",ofstream::app);
	// metafile << "run num is " << runNum << endl;
	// metafile << "phase two spent(priority queue) " << cpu_time_used << endl;
	// metafile << "\n";
	// cout << "PriorityQueue: spent " << cpu_time_used * 1000 <<" ms" << endl;
	// cout << "LinearScan: spent " << cpu_time_used * 1000 <<" ms" << endl;
}


// class LinearScanMember {
//     Run *myRun;       
//     Record rec;  } 
void BigQ::LinearScan(vector<Run> &runs){
	ComparisonEngine comp;
	Sorter sorter(sortorder);
	LinearScanMember tempLSM;
	Record *minRec;
	//vector to store tagged runs for linear scan
	vector<LinearScanMember> run_vector;
	run_vector.reserve(runNum);
	//traverse runs vector to initial the vector
	for (auto it=runs.begin(); it!=runs.end(); ++it){
		tempLSM.myRun = &(*it);
		//perform move first to get first page
		it->MoveFirst();
		it->GetNext(tempLSM.rec);
		run_vector.push_back(move(tempLSM));
	}
	//iterator to store the minimal node for later use
	auto minIt = run_vector.begin();
	while (0 == run_vector.empty()) {
		minRec = &(minIt->rec);
		//linear scan the vector
		for (auto itl = run_vector.begin(); itl != run_vector.end(); ++itl){
			//if the current record less than the min record
			if (comp.Compare (minRec, &(itl->rec), &sortorder) > 0) {
				//make it to min record and store the min node(iterator)
				minRec = &(itl->rec);
				minIt = itl;
			}
		}
		out.Insert(minRec);
		//if the run with minimal record is empty
		if (0 == (minIt->myRun)->GetNext(minIt->rec)) {
			//erase the run
 			run_vector.erase(minIt);
 			//set the minimal node to the begin node
 			minIt = run_vector.begin();
 		}
	}
}

// class QueueMember {
//     int runID;        
//     Record rec; 	 } 
void BigQ::PriorityQueue (vector<Run> &runs) {
	int minID = 0;	
	QueueMember tempQM;
	Sorter sorter(sortorder);
	//priority queue of first record of each run
	//std priority queue don't support "move" semantic pop in 2015
	Queue pqueue(sorter);
	//reserve place for internal vector in queue
	pqueue.reserve(runNum);
	//traverse runs vector to initial the priority queue
	for (auto it=runs.begin(); it!=runs.end(); ++it) {
		tempQM.runID = it->GetRunID();
		//perform move first to get first page
		it->MoveFirst();
		it->GetNext(tempQM.rec);
		//"move" semantic push
		pqueue.push(tempQM);
	}
	while (0 == pqueue.empty()) {
		//"move" semantic pop, pop the top element and return move(element)
		tempQM = pqueue.pop();
		//store the run ID of the smallest record
		minID = tempQM.runID;
		//insert the smallest record to output pipe
		out.Insert(&tempQM.rec);
		//pop the minimal record
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

//copy assignment operator
Run& Run::operator = (const Run &r) {
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	curPage = r.curPage;
  	return *this;
}

//move constructor
Run::Run (Run &&r) {
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	curPage = move(r.curPage);
}

//move assignment operator
Run& Run::operator = (Run &&r) {
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	runsFile = r.runsFile;
  	curPage = move(r.curPage);
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

Queue::Queue(Sorter &s) : sorter(s) { }
    //reserve place in vector to increase performance
void Queue::reserve(int n) {
    qm.reserve(n);
}
//return if the queue if empty
bool Queue::empty() {
    return qm.empty();
}
//push element into queue
void Queue::push(QueueMember &element) {
    qm.push_back(move(element));        
    push_heap(qm.begin(), qm.end(), sorter);
}
//pop element from queue
QueueMember Queue::pop() {
    pop_heap(qm.begin(), qm.end(), sorter);
    QueueMember result = move(qm.back());
    qm.pop_back();
    return move(result);
}

QueueMember::QueueMember () : runID(0), rec() { }   

//move constructor
QueueMember::QueueMember (QueueMember &&qm){
	runID = qm.runID;       
    rec = move(qm.rec);
}

//move assignment operator
QueueMember & QueueMember::operator = (QueueMember &&qm) {
	runID = qm.runID;       
    rec = move(qm.rec);
    return *this;
}

LinearScanMember::LinearScanMember () : myRun(nullptr), rec() { }

//move constructor
LinearScanMember::LinearScanMember (LinearScanMember &&lm) {
	myRun = lm.myRun;
	lm.myRun = nullptr;
	rec = move(lm.rec);
}

//move assignment operator
LinearScanMember & LinearScanMember::operator = (LinearScanMember &&lm) {
	myRun = lm.myRun;
	lm.myRun = nullptr;
	rec = move(lm.rec);
	return *this;
}

/*
 *Discard 2015.03.21
 *Slower than vector linear scan
 *because the main overhead is scan, not delete
 *and vector has less cache miss
 */
/******************************linear scan with list*********************************
void BigQ::LinearScan(vector<Run> &runs){
	ComparisonEngine comp;
	Sorter sorter(sortorder);
	LinearScanMember tempLM;
	Record *minRec;
	//list to store tagged runs for linear scan
	list<LinearScanMember> run_vector;
	//traverse runs vector to initial the list
	for (auto it=runs.begin(); it!=runs.end(); ++it){
		tempLM.myRun = &(*it);
		//perform move first to get first page
		it->MoveFirst();
		it->GetNext(tempLM.rec);
		run_vector.push_front(move(tempLM));
	}
	//iterator to store the minimal node for later use
	auto minIt = run_vector.begin();
	while (0 == run_vector.empty()) {
		minRec = &(minIt->rec);
		//linear scan the list
		for (auto itl = run_vector.begin(); itl != run_vector.end(); ++itl){
			//if the current record less than the min record
			if (comp.Compare (minRec, &(itl->rec), &sortorder) > 0) {
				//make it to min record and store the min node(iterator)
				minRec = &(itl->rec);
				minIt = itl;
			}
		}
		out.Insert(minRec);
		//if the run with minimal record is empty
		if (0 == (minIt->myRun)->GetNext(minIt->rec)) {
			//erase the run
 			run_vector.erase(minIt);
 			//set the minimal node to the begin node
 			minIt = run_vector.begin();
 		}
	}
}
******************************linear scan with list*********************************/