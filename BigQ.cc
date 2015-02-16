#include "BigQ.h"

BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sorder, int rl) {

	// read data from in pipe sort them into runlen pages
	
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
 	sortorder = sorder;
 	runlen = rl;
 	runNum = 0;
 	TPM_MergeSort(in, out);
//	pthread_t workthread;
//	pthread_create (&workthread, NULL, TPM_MergeSort, (void *)&input);	

    // finally shut down the out pipe	
}

BigQ::~BigQ () {
}

//two phase multiway merge sort
void BigQ::TPM_MergeSort(Pipe &in, Pipe &out){//two phase multiway merge sort
	char filename[256]="dbfile/tempfile";
	runsFile.Open(0, filename);
	vector<Run> runs;
	FirstPhase(in, runs);
	SecondPhase(out, runs);
	out.ShutDown ();
	runsFile.Close();
}

void BigQ::SortInRun(vector<Record> &oneRunRecords){
	Sorter runSorter(sortorder);
	sort(oneRunRecords.begin(), oneRunRecords.end(), runSorter);
}

void BigQ::WriteRunToFile(vector<Record> &oneRunRecords){	
	Page tempPage;
	Record tempRec;
	int tempIndex;
//	Schema mySchema ("catalog", "nation");
	if(runsFile.GetLength() == 0){
		tempIndex = 0;      
	}
	//if the file is not empty, file length-2 is the last page, will not 
	//write two oneRunRecords to the same page
	else if(runsFile.GetLength() > 0){
		tempIndex = runsFile.GetLength() - 1;
		//runsFile.GetPage(&tempPage, tempIndex);		 **this cause out of bound get page
	}
	else{ //length less than 0, something wrong with current file
		cerr << "File length less than 0 in DBFile add\n";
		exit (1);
	}

	while (oneRunRecords.size() > 0) {//while there are records in this oneRunRecords
		tempRec = oneRunRecords.back();
//		tempRec.Print(&mySchema);
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
void BigQ::FirstPhase(Pipe &in, vector<Run> &runs){ 
	int curLen = 0;
	int curSize = 0;
	Record tempRec;

	vector<Record> oneRunRecords;
	

	while(in.Remove(&tempRec)){
		//check if a single record larger than a page
		if(tempRec.Size() > PAGE_SIZE){ 
			cerr << "record larger than a page in TPMMS first phase\n";
			exit(1);
		}

		//the sum of record size is larger than oneRunRecords size
		curSize += tempRec.Size();
//cout << "cur size" << curSize << "\n" << endl;
		if(curSize > PAGE_SIZE){ 
			curLen++;			
			curSize = tempRec.Size();
		}
	
		//current length greater than oneRunRecords length, the current oneRunRecords is full
		if(curLen > runlen){
			//sort the current run
			SortInRun(oneRunRecords);
			//write the sorted run to temporary file
			WriteRunToFile(oneRunRecords);
cout << "curLen > runlen" << "\n" << endl;
			runs.push_back(Run(runNum, runNum * runlen, runlen, &runsFile));
			oneRunRecords.clear();
			curLen = 0;
			runNum++;
		}
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
void BigQ::SecondPhase(Pipe &out, vector<Run> &runs){	
	Page tempPage;
	Record minRec;
	Sorter sorter(sortorder);
	int minID = 0;
	int runCount = 0;

	int test_counter = 0;

	vector<Run>::iterator it;

	//initial mincRec to be the first record of first run
	minRec = runs[0].GetRecord();
	//while there ara runs not exhausted 
	//minRec.Print(&mySchema);
	while(runCount < runNum){
		//traverse all runs
		for (it=runs.begin(); it!=runs.end(); ++it){
			//if the run is not exhausted
			if(it->isExhaust()){
    			//if current record less than min record
    			if(sorter(minRec, it->GetRecord()) < 0){
					minRec = it->GetRecord();
					minID = it->GetRunID();					
				}				
    		}
		}
		//insert the smallest record to out pipe
		out.Insert(&minRec);
		test_counter++;
		cout << "\t!@#!#" << test_counter <<endl;
		//if the run is exhausted
		if(!runs[minID].GetNext()){
			runs[minID].setExhaust();
			//count the exhausted runs
			runCount++;
		}
	}
}

 Run::Run(int ID, int beg, int rl, File *rf){
 	runID = ID;
 	curRunLen = rl;
  	runBegIndex = beg;
  	runCurIndex = beg;
  	runsFile = rf;
  	exhaust = 1;
  	runsFile->GetPage(&curPage, runBegIndex);
  	this->GetNext();
 }

 Run::Run(const Run &r){
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	exhaust = r.exhaust;
  	runsFile = r.runsFile;
  	runsFile->GetPage(&curPage, runBegIndex);
  	this->GetNext();
 }

 Run::~Run(){ 	
 }

 Run& Run::operator = (const Run r){
 	runID = r.runID;
 	curRunLen = r.curRunLen;
  	runBegIndex = r.runBegIndex;
  	runCurIndex = r.runCurIndex;
  	exhaust = r.exhaust;
  	runsFile = r.runsFile;
  	runsFile->GetPage(&curPage, runBegIndex);
  	this->GetNext();
  	return *this;
 }

int Run::GetRunID(){
	return runID;
}

Record Run::GetRecord(){
	return curRec;
}

int Run::GetNext(){
 	while(runCurIndex < runBegIndex + curRunLen){
 		if(curPage.GetFirst(&curRec)){
 			return 1;
 		}else{
 			if(++runCurIndex < runBegIndex + curRunLen){
 				runsFile->GetPage(&curPage, runCurIndex); 
 			}else{
 				break;
 			}			
 		}
 	}
 	return 0;
 }

 void Run::setExhaust(){
 	exhaust = 0;
 }

 bool Run::isExhaust(){
 	return exhaust;
 }