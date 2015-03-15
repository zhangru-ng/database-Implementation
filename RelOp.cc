#include "RelOp.h"

// @tested
/*************************************SelectFile*********************************************/
SelectFile::SelectFile() : inFile(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr) { }

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	StartInternalThread();
}

void SelectFile::WaitUntilDone () {
	 WaitForInternalThreadToExit();
}

void SelectFile::Use_n_Pages (int n) {
	// runlen = n;
}

// performs a scan of the underlying file, and for every tuple accepted by the CNF, 
// it stuffs the tuple into the pipe as output
void* SelectFile::InternalThreadEntry() {
	Record tempRec;
	ComparisonEngine comp;
	// assume that this file is all set up, it has been opened and is ready to go
	inFile->MoveFirst();
	while( 1 == inFile->GetNext(tempRec) ){
		if( 1 == comp.Compare(&tempRec, literal, selOp) ){
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);
}
/*************************************SelectFile*********************************************/


/*************************************SelectPipe*********************************************/
SelectPipe::SelectPipe() : inPipe(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr) { }

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	StartInternalThread();
}

void SelectPipe::WaitUntilDone () {
	WaitForInternalThreadToExit();
}

void SelectPipe::Use_n_Pages (int n) { 
	// runlen = n;
}

// applies that CNF to every tuple that comes through the pipe, and every
// tuple that is accepted is stuffed into the output pipe
void* SelectPipe::InternalThreadEntry() {
	Record tempRec;
	ComparisonEngine comp;
	while( 1 == inPipe->Remove(&tempRec) ){
		if( 1 == comp.Compare(&tempRec, literal, selOp) ){
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);
}
/*************************************SelectPipe*********************************************/

// @tested
/**************************************Project***********************************************/
Project::Project() : inPipe(nullptr), outPipe(nullptr), keepMe(nullptr), numAttsInput(0), numAttsOutput(0) { }

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	StartInternalThread();
}

void Project::WaitUntilDone () {
	WaitForInternalThreadToExit();
}

void Project::Use_n_Pages (int n) {
	// runlen = n;
}

// Project takes an input pipe and an output pipe as input. It also takes an array of integers keepMe 
// as well as the number of attributes for the records coming through the input pipe and the number of 
// attributes to keep from those input records. The array of integers tells Project which attributes to 
// keep from the input records, and which order to put them in.
void* Project::InternalThreadEntry() {
	Record tempRec;
	while( 1 == inPipe->Remove(&tempRec) ){
		//Project (int *attsToKeep, int numAttsToKeep, int numAttsNow);
		tempRec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&tempRec);		
	}	
	outPipe->ShutDown();
	pthread_exit(nullptr);
}
/**************************************Project***********************************************/


/***************************************Join*************************************************/
Join::Join() : inPipeL(nullptr), inPipeR(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr), numAttsLeft(0), numAttsRight(0), numAttsToKeep(0)  { }

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	StartInternalThread();
}

void Join::WaitUntilDone (){
	WaitForInternalThreadToExit();

}

void Join::Use_n_Pages (int n){
	runlen = n/2;
}

// Join takes two input pipes, an output pipe, and a CNF, and joins all of the records from
// the two pipes according to that CNF. Join should use a BigQ to store all of the tuples
// coming from the left input pipe, and a second BigQ for the right input pipe, and then
// perform a merge in order to join the two input pipes. You’ll create the OrderMakers
// for the two BigQ’s using the CNF (the function GetSortOrders will be used to create
// the OrderMakers). If you can’t get an appropriate pair of OrderMakers because the
// CNF can’t be implemented using a sort-merge join (due to the fact it does not have an
// equality check) then your Join operation should default to a block-nested loops join
void* Join::InternalThreadEntry() {
	OrderMaker sortorderL, sortorderR;
	if (0 == selOp->GetSortOrders (sortorderL, sortorderR)) {
		BlockNestedJoin(); 
	} else {
	 	SortMergeJoin(sortorderL, sortorderR);
	}	
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

void Join::BlockNestedJoin() {
	Record tempRecL, tempRecR, joinRec;
	vector<Record> leftRecords;
	vector<Record> rightRecords;
	leftRecords.reserve(1024);
	rightRecords.reserve(1024);

	ComparisonEngine comp;
	bool fitInMemory = true;

	//create the temporary file to store tuples remove from input pipe
	char lfname[80] = "/cise/tmp/rui/temp/JLtemp_XXXXXX";
	char rfname[80] = "/cise/tmp/rui/temp/JRtemp_XXXXXX";
	mkstemp(lfname);
	mkstemp(rfname);
	DBFile leftFile, rightFile;
	leftFile.Create(lfname, heap, nullptr);
	rightFile.Create(rfname, heap, nullptr);

	int *attsToKeep;

	if (inPipeL->Remove(&tempRecL) && inPipeR->Remove(&tempRecR)) {

		numAttsLeft = tempRecL.GetNumAtts();
		numAttsRight = tempRecR.GetNumAtts();		
		numAttsToKeep = numAttsLeft + numAttsRight;
		attsToKeep = new int[numAttsToKeep];
		for (int i = 0; i < numAttsToKeep; ++i) {
			if (i >= numAttsLeft) {
				attsToKeep[i] = i - numAttsLeft;
				continue;
			}
			attsToKeep[i] = i;
		}
		
		leftRecords.push_back(tempRecL);
		rightRecords.push_back(tempRecR);
		int totSize = tempRecL.Size() + tempRecR.Size();
		bool smaller;
		bool left, right;
		long maxsize = 2 * runlen * PAGE_SIZE;

		while (1) {
			left = inPipeL->Remove(&tempRecL);
			right = inPipeR->Remove(&tempRecR);

			//if both left and right relation reach the end 
			if ( false == (left || right) ) {
				break;
			}

			//if both left and right relation reach the end, test which one is smaller
			if ( false == left && right) {
				//left == 0 ? left:right;  0 for left,  1 for right
				smaller = left == 0 ? LEFT : RIGHT;
			}

			if (left) {
				leftRecords.push_back(tempRecL);
				totSize += tempRecL.Size();
			}

			if (right) {
				rightRecords.push_back(tempRecR);
				totSize += tempRecR.Size();
			}

			if (totSize > maxsize) {
				fitInMemory = false;
				WriteToFile(leftRecords, leftFile);
				WriteToFile(rightRecords, rightFile);
				leftRecords.clear();
				rightRecords.clear();
				// if(left && right){
				// 	WriteToFile(leftRecords, leftFile);
				// 	WriteToFile(rightRecords, rightFile);
				// 	leftRecords.clear();
				// 	rightRecords.clear();
				// }else if(left){
				// 	WriteToFile(leftRecords, leftFile);
				// 	leftRecords.clear();
				// }else if(right){
				// 	WriteToFile(rightRecords, rightFile);
				// 	rightRecords.clear();
				// }				
				totSize = 0;
			}			
		}
			
		//if the size of records do not exceed the memory size setting by Use_n_Pages (int n)
		//nested scan left and right vector to join records accepted by CNF:selOp
		if (true == fitInMemory) {
			if (RIGHT == smaller) {
				FitInMemoryJoin(leftRecords, rightRecords, attsToKeep);
			}else {
				FitInMemoryJoin(rightRecords, leftRecords, attsToKeep);
			}			
		} else {
			WriteToFile(leftRecords, leftFile);
			WriteToFile(rightRecords, rightFile);
			if (RIGHT == smaller) {
				JoinRecInFile(leftFile, rightFile, attsToKeep);
			} else {
				JoinRecInFile(rightFile, leftFile, attsToKeep);
			}
		}		
	}			

	leftFile.Close();
	rightFile.Close();
	remove(lfname);
	remove(rfname);
}

void Join::WriteToFile(vector<Record> &run, DBFile &file){
	for (vector<Record>::iterator it = run.begin();it != run.end(); ++it) {
		file.Add(*it);
	} 
}

void Join::FitInMemoryJoin(vector<Record> &leftRecords, vector<Record> &rightRecords, int *attsToKeep) {
	Record joinRec;
	ComparisonEngine comp;
	for(vector<Record>::iterator itL = leftRecords.begin(); itL != leftRecords.end(); ++itL){
		for(vector<Record>::iterator itR = rightRecords.begin(); itR != rightRecords.end(); ++itR){
			if(comp.Compare(&(*itL), &(*itR), literal, selOp)){
				joinRec.MergeRecords (&(*itL), &(*itR), numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
				outPipe->Insert(&joinRec);
			}	
		}
	}
}

void Join::JoinRecInFile(DBFile &outter, DBFile &inner, int * attsToKeep) {
	Record tempRec, joinRec;
	ComparisonEngine comp;
	Page tempPage;
	vector<Record> block;
	outter.MoveFirst();
	inner.MoveFirst();
	block.reserve(65536);

	size_t index = 0;
	size_t length = outter.GetLength() - 2;
	while (index <= length) {
		FillBlock(outter, block, 2 * runlen - 1, index);
		while (inner.GetNext(tempRec)) {						
			for(vector<Record>::iterator it = block.begin(); it != block.end(); ++it){
				if(comp.Compare(&(*it), &tempRec, literal, selOp)){
					joinRec.MergeRecords (&(*it), &tempRec, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
					outPipe->Insert(&joinRec);
				}	
			}		
		}
		inner.MoveFirst();
	}		
}

// void Join::FillBlock(File &file, vector<Record> &block, int n, int &index){
// 	Record tempRec;
// 	for(int i = 0; i < n; ++i){
// 		if (index < file.GetLength() - 2) {
// 			file.GetNext(tempRec)
// 			block.push_back(tempRec);
// 		}	
// 	}
// }

void Join::SortMergeJoin(OrderMaker &sortorderL, OrderMaker &sortorderR) {
	Record tempRecL, tempRecR;
	ComparisonEngine comp;
	Pipe outputL(buffsz);
	Pipe outputR(buffsz);
	BigQ bqL(*inPipeL, outputL, sortorderL, runlen);
	BigQ bqR(*inPipeR, outputR, sortorderR, runlen);
	int *attsToKeep = nullptr;
	//if both the output pipes have at least one record
	if(outputL.Remove(&tempRecL) && outputR.Remove(&tempRecR)) {
		numAttsLeft = tempRecL.GetNumAtts();
		numAttsRight = tempRecR.GetNumAtts();
		numAttsToKeep = numAttsLeft + numAttsRight;
		attsToKeep = new int[numAttsToKeep];
		for(int i = 0; i < numAttsToKeep; ++i) {
			if(i >= numAttsLeft) {
				attsToKeep[i] = i - numAttsLeft;
				continue;
			}
			attsToKeep[i] = i;
		}

		while(1){
			// if the left record is less than right record
			if (comp.Compare(&tempRecL, &sortorderL, &tempRecR, &sortorderR) < 0) {
				//get next record in left output pipe for next comparion, if no record left then break
				if (0 == outputL.Remove(&tempRecL)) {				
					break;
				}
			}
			// if the left record is greater than right record
			else if (comp.Compare(&tempRecL, &sortorderL, &tempRecR, &sortorderR) > 0) {
				//get next record in right output pipe for next comparion, if no record left then break
				if (0 == outputR.Remove(&tempRecR)) {					
					break;
				}
			}
			// if left record equal to right record
			else {
				int isEnd = OutputTuple(tempRecL, tempRecR, outputL, outputR, sortorderL, sortorderR, attsToKeep);				
				//get next right record  for next comparion, if no record left then break
				if (0 == isEnd) {					
					break;
				}
			}
		}		
	}
	delete[] attsToKeep;
}

int Join::OutputTuple(Record &left, Record &right, Pipe &outputL, Pipe &outputR, OrderMaker &sortorderL, OrderMaker &sortorderR, int *attsToKeep) {
	Record tempRecL, tempRecR, joinRec;
	ComparisonEngine comp;
	vector<Record> leftRecords;
	vector<Record> rightRecords;
	leftRecords.reserve(1024);
	rightRecords.reserve(1024);
	leftRecords.push_back(left);
	rightRecords.push_back(right);
	int isLeftEnd, isRightEnd;
	//collect all equal records in left pipe
	while( isLeftEnd = outputL.Remove(&tempRecL) ){
		if( 0 == comp.Compare(&tempRecL, &sortorderL, &right, &sortorderR)){
			leftRecords.push_back(tempRecL);
		}else{
			break;
		}
	}
	//collect all equal records in right pipe
	while( isRightEnd = outputR.Remove(&tempRecR) ){
		if( 0 == comp.Compare(&left, &sortorderL, &tempRecR, &sortorderR)){
			rightRecords.push_back(tempRecR);
		}else{
			break;
		}
	}
	//merge all matched records
	for(vector<Record>::iterator itL = leftRecords.begin(); itL != leftRecords.end(); ++itL){
		for(vector<Record>::iterator itR = rightRecords.begin(); itR != rightRecords.end(); ++itR){
			joinRec.MergeRecords (&(*itL), &(*itR), numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
			outPipe->Insert(&joinRec);
		}
	}	
	int isEnd = isLeftEnd && isRightEnd;
	//if both relations do not reach the end
	if(isEnd){
		// assign the first unequal record for next round comparison	
		left.Copy(&tempRecL);
		right.Copy(&tempRecR);
	}
	return isEnd;
}
/***************************************Join*************************************************/

// @tested
/**********************************DuplicateRemoval******************************************/
DuplicateRemoval::DuplicateRemoval() : inPipe(nullptr), outPipe(nullptr), mySchema(nullptr) { }

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema; 
	StartInternalThread();
}

void DuplicateRemoval::WaitUntilDone (){
	WaitForInternalThreadToExit();
}

void DuplicateRemoval::Use_n_Pages (int n){
	runlen = n;
}

// DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
// tuples coming through the input pipe, and does a duplicate removal. That is, everything
// that somes through the output pipe will be distinct. It will use the BigQ class to do the
// duplicate removal. The OrderMaker that will be used by the BigQ (which you’ll need
// to write some code to create) will simply list all of the attributes from the input tuples.
void* DuplicateRemoval::InternalThreadEntry(){
	Record tempRec;
	//last unduplicate record
	Record lastRec;
	OrderMaker DRorder(mySchema);
	ComparisonEngine comp;
	Pipe output(buffsz);	
	BigQ bq(*inPipe, output, DRorder, runlen);
	//initial the first lastRec
	if(output.Remove(&tempRec)){
		lastRec = tempRec;
		outPipe->Insert(&tempRec);
	}
	while( output.Remove(&tempRec) ){
		if( 0 != comp.Compare(&tempRec, &lastRec, &DRorder)){
			//pipe Insert will consume tempRec, have to save it before Insert
			lastRec = tempRec;
			outPipe->Insert(&tempRec);
		}					
	}
	//shut down output pipe
	outPipe->ShutDown();
	pthread_exit(nullptr);
}
/**********************************DuplicateRemoval******************************************/

// @tested
/***************************************Sum**************************************************/
Sum::Sum() : inPipe(nullptr), outPipe(nullptr), computeMe(nullptr), intSum(0), doubleSum(0) { }

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe; 
	StartInternalThread();
}

void Sum::WaitUntilDone (){
	WaitForInternalThreadToExit();
}

void Sum::Use_n_Pages (int n){
	// runlen = n;
}

// Sum computes the SUM SQL aggregate function over the input pipe, and puts a single tuple
// into the output pipe that has the sum. The function over each tuple that is summed is 
// stored in an instance of the Function class that is also passed to Sum as an argument
void* Sum::InternalThreadEntry(){
	// Type Apply (Record &toMe, int &intResult, double &doubleResult);	
	Record tempRec;
	string result;
	struct Attribute att;
	if(inPipe->Remove(&tempRec)){
		att.name = strdup("SUM");
		att.myType = computeMe->Apply(tempRec, intSum, doubleSum);	
		//check attribute type to decide following step
		switch(att.myType){
			case Int:
			//int sumation
				result = IntSum();
				break;
			case Double: 
			//double sumation
				result = DoubleSum();
				break;				
		}		
		//create schema contain one attribute "SUM" for output record 
		Schema sumSchema("tempSchema", 1, &att);
		//ComposeRecord use terminate symbol '|' to calculate attribute length
		result.append("|");
		tempRec.ComposeRecord (&sumSchema, result.c_str());
		outPipe->Insert(&tempRec);	
		//free memory allocate by strdup
		free(att.name);	
	}
	//shut down output pipe
	outPipe->ShutDown();	
	pthread_exit(nullptr);	
}

string Sum::IntSum(){
	int intResult;
	double dummy;
	Record tempRec;
	while( inPipe->Remove(&tempRec) ){		
		computeMe->Apply(tempRec, intResult, dummy);		
		intSum += intResult;
	}		
	return to_string(intSum);		
}


string Sum::DoubleSum(){
	double doubleResult;
	int dummy;
	Record tempRec;
	while( inPipe->Remove(&tempRec) ){		
		computeMe->Apply(tempRec, dummy, doubleResult);		
		doubleSum += doubleResult;
	}		
	return to_string(doubleSum);
}
/***************************************Sum**************************************************/


/*************************************GroupBy************************************************/
GroupBy::GroupBy() : inPipe(nullptr), outPipe(nullptr), groupAtts(nullptr), computeMe(nullptr) { }

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	StartInternalThread();
}

void GroupBy::WaitUntilDone (){
	WaitForInternalThreadToExit();
}

void GroupBy::Use_n_Pages (int n){
	runlen = n;
}

// GroupBy is a lot like Sum, except that it does grouping, and then puts one sum into the
// output pipe for each group. Every tuple put into the output pipe has a sum as the first
// attribute, followed by the values for each of the grouping attributes as the remainder of
// the attributes. The grouping is specified using an instance of the OrderMaker class that
// is passed in. The sum to compute is given in an instance of the Function class.
void* GroupBy::InternalThreadEntry(){
	Record tempRec, lastRec;

	int numGroupAtts = groupAtts->GetNumAtts();
	int *attsToKeep = new int[numGroupAtts + 1];
	{
		int *Atts = groupAtts->GetAtts();	
		attsToKeep[0] = 0;
		for(int i = 0; i < numGroupAtts; ++i){
			attsToKeep[i+1] = Atts[i];
		}	
	}
	
	Pipe output(buffsz);	
	BigQ bq(*inPipe, output, *groupAtts, runlen);

	if(output.Remove(&tempRec)){
		struct Attribute att;
		att.name = strdup("SUM");
		att.myType = computeMe->Apply(tempRec, intSum, doubleSum);
		Schema sumSchema("tempSchema", 1, &att);
		int numAtts = tempRec.GetNumAtts();
		lastRec	= tempRec;
		switch(att.myType){
			case Int:
			//int group
				GroupInt(output, lastRec, numAtts, numGroupAtts, attsToKeep, sumSchema);
				break;
			case Double: 
			//double group
				GroupDouble(output, lastRec, numAtts, numGroupAtts, attsToKeep, sumSchema);
				break;				
		}		
	}	
	delete[] attsToKeep;
	outPipe->ShutDown();
	pthread_exit(nullptr);		
}

void GroupBy::GroupInt(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *attsToKeep, Schema &sumSchema){
	Record tempRec, sumnRecord, groupRec;
	ComparisonEngine comp;
	int intResult;
	double dummy;
	string result;
	while( output.Remove(&tempRec) ){
		if( 0 == comp.Compare(&tempRec, &lastRec, groupAtts)){
			computeMe->Apply(tempRec, intResult, dummy);
			intSum += intResult;
		}else{
			result = to_string(intSum);
			//ComposeRecord use terminate symbol '|' to calculate attribute length
			result.append("|");
			sumnRecord.ComposeRecord (&sumSchema, result.c_str());
			//compose record				
			groupRec.MergeRecords (&sumnRecord, &lastRec, 1, numAtts, attsToKeep, numGroupAtts + 1, 1);
			outPipe->Insert(&groupRec);		
			computeMe->Apply(tempRec, intSum, dummy);	
			lastRec.Consume(&tempRec);
		}					
	}
	result = to_string(intSum);
	//ComposeRecord use terminate symbol '|' to calculate attribute length
	result.append("|");
	sumnRecord.ComposeRecord (&sumSchema, result.c_str());
	//compose record				
	groupRec.MergeRecords (&sumnRecord, &lastRec, 1, numAtts, attsToKeep, numGroupAtts + 1, 1);
	outPipe->Insert(&groupRec);	
}

void GroupBy::GroupDouble(Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *attsToKeep, Schema &sumSchema){
	Record tempRec, sumnRecord, groupRec;
	ComparisonEngine comp;
	int dummy;
	double doubleResult;
	string result;
	while( output.Remove(&tempRec) ){
		if( 0 == comp.Compare(&tempRec, &lastRec, groupAtts)){
			computeMe->Apply(tempRec, dummy, doubleResult);
			doubleSum += doubleResult;
		}else{
			result = to_string(doubleSum);
			//ComposeRecord use terminate symbol '|' to calculate attribute length
			result.append("|");
			sumnRecord.ComposeRecord (&sumSchema, result.c_str());
			//compose record				
			groupRec.MergeRecords (&sumnRecord, &lastRec, 1, numAtts, attsToKeep, numGroupAtts + 1, 1);
			outPipe->Insert(&groupRec);		
			computeMe->Apply(tempRec, dummy, doubleSum);	
			lastRec.Consume(&tempRec);
		}					
	}
	result = to_string(doubleSum);
	//ComposeRecord use terminate symbol '|' to calculate attribute length
	result.append("|");
	sumnRecord.ComposeRecord (&sumSchema, result.c_str());
	//compose record				
	groupRec.MergeRecords (&sumnRecord, &lastRec, 1, numAtts, attsToKeep, numGroupAtts + 1, 1);
	outPipe->Insert(&groupRec);			
}
/*************************************GroupBy************************************************/

// @tested
/*************************************WriteOut***********************************************/
WriteOut::WriteOut() : inPipe(nullptr), outFile(nullptr), mySchema(nullptr){ }

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	StartInternalThread();
}

void WriteOut::WaitUntilDone (){
	WaitForInternalThreadToExit();
}

void WriteOut::Use_n_Pages (int n){
	// runlen = n;
}

// WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to write
// text version of the output records to the file.
void* WriteOut::InternalThreadEntry(){
	Record tempRec;
	int n = mySchema->GetNumAtts();
	Attribute *atts = mySchema->GetAtts();

	while( inPipe->Remove(&tempRec) ){
		// loop through all of the attributes
		for (int i = 0; i < n; i++) {
			// print the attribute name
			fprintf(outFile, "%s: [", atts[i].name);
			// use the i^th slot at the head of the record to get the
			// offset to the correct attribute in the record
			int pointer = ((int *) tempRec.bits)[i + 1];
			// here we determine the type, which given in the schema;
			// depending on the type we then print out the contents
			// first is integer
			if (atts[i].myType == Int) {
				int *myInt = (int *) &(tempRec.bits[pointer]);
				fprintf(outFile, "%d", *myInt);
			// then is a double
			} else if (atts[i].myType == Double) {
				double *myDouble = (double *) &(tempRec.bits[pointer]);
				fprintf(outFile, "%f", *myDouble);
			// then is a character string
			} else if (atts[i].myType == String) {
				char *myString = (char *) &(tempRec.bits[pointer]);
				fprintf(outFile, "%s", myString);
			} 
			fprintf(outFile, "]");
			// print out a comma as needed to make things pretty
			if (i != n - 1) {
				fprintf(outFile, ", ");
			}

		}
		fprintf(outFile, "\n");		
	}		
	pthread_exit(nullptr);
}
/*************************************WriteOut***********************************************/