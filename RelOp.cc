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
	runlen = n;
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
Join::Join() : inPipeL(nullptr), inPipeR(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr){ }

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
	runlen = n;
}

// Join takes two input pipes, an output pipe, and a CNF, and joins all of the records from
// the two pipes according to that CNF. Join should use a BigQ to store all of the tuples
// coming from the left input pipe, and a second BigQ for the right input pipe, and then
// perform a merge in order to join the two input pipes. You’ll create the OrderMakers
// for the two BigQ’s using the CNF (the function GetSortOrders will be used to create
// the OrderMakers). If you can’t get an appropriate pair of OrderMakers because the
// CNF can’t be implemented using a sort-merge join (due to the fact it does not have an
// equality check) then your Join operation should default to a block-nested loops join
void* Join::InternalThreadEntry(){
	Record tempRecL, tempRecR, joinedRec;
	ComparisonEngine comp;
	OrderMaker sortorderL, sortorderR;
	if( 0 == selOp->GetSortOrders (sortorderL, sortorderR) ){
		// block-nested loops join
	}
	Pipe *outputL = new Pipe(buffsz);
	Pipe *outputR = new Pipe(buffsz);
	BigQ *bqL = new BigQ(*inPipeL, *outputL, sortorderL, runlen);
	BigQ *bqR = new BigQ(*inPipeR, *outputR, sortorderR, runlen);
	while( outputL->Remove(&tempRecL) && outputR->Remove(&tempRecR) ){
		if( comp.Compare(&tempRecL, &tempRecR, literal, selOp)){
	//???	joinedRec.MergeRecords (&tempRecL, &tempRecR, int numAttsLeft, int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);
			outPipe->Insert(&joinedRec);		
		}		
	}	
	outPipe->ShutDown();
	pthread_exit(nullptr);
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
	DRorder.Print();
	ComparisonEngine comp;
	Pipe *output = new Pipe(buffsz);	
	BigQ *bq = new BigQ(*inPipe, *output, DRorder, runlen);
	//initial the first lastRec
	if(output->Remove(&tempRec)){
		lastRec = tempRec;
		outPipe->Insert(&tempRec);
	}
	while( output->Remove(&tempRec) ){
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
Sum::Sum() : inPipe(nullptr), outPipe(nullptr), computeMe(nullptr) { }

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
	runlen = n;
}

// Sum computes the SUM SQL aggregate function over the input pipe, and puts a single tuple
// into the output pipe that has the sum. The function over each tuple that is summed is 
// stored in an instance of the Function class that is also passed to Sum as an argument
void* Sum::InternalThreadEntry(){
	// Type Apply (Record &toMe, int &intResult, double &doubleResult);
	Record tempRec;
	int intResult, intSum;
	double doubleResult, doubleSum;
	string result;
	struct Attribute att;
	if(inPipe->Remove(&tempRec)){
		att.name = strdup("SUM");
		att.myType = computeMe->Apply(tempRec, intResult, doubleResult);	
		//check attribute type to decide following step
		switch(att.myType){
			case Int:
			//int sumation
				intSum = intResult;
				while( inPipe->Remove(&tempRec) ){		
					computeMe->Apply(tempRec, intResult, doubleResult);		
					intSum += intResult;
				}		
				result = to_string(intSum);		
				break;
			case Double: 
			//double sumation
				doubleSum = doubleResult;
				while( inPipe->Remove(&tempRec) ){		
					computeMe->Apply(tempRec, intResult, doubleResult);		
					doubleSum += doubleResult;
				}		
				result = to_string(doubleSum);
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
	ComparisonEngine comp;

	int intResult = 0;
	double doubleResult = 0;
	string result;
	Pipe *output = new Pipe(buffsz);	
	BigQ *bq = new BigQ(*inPipe, *output, *groupAtts, runlen);

	if(output->Remove(&tempRec)){
		lastRec	= tempRec;
		outPipe->Insert(&tempRec);
	}
	while( output->Remove(&tempRec) ){
		if( 1 == comp.Compare(&tempRec, &lastRec, groupAtts)){
			computeMe->Apply(tempRec, intResult, doubleResult);				
		}else{
			//compose record				
			outPipe->Insert(&tempRec);		
			intResult = 0;
			doubleResult = 0;
			computeMe->Apply(tempRec, intResult, doubleResult);	
			lastRec.Consume(&tempRec);
		}					
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);		
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
	runlen = n;
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