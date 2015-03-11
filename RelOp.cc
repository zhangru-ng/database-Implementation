#include "RelOp.h"

// performs a scan of the underlying file, and for every tuple accepted by the CNF, 
// it stuffs the tuple into the pipe as output
SelectFile::SelectFile() : inFile(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr) { }

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inFile = &inFile;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, nullptr);
}

void SelectFile::Use_n_Pages (int runlen) {

}

void* SelectFile::thread_wrapper (void* arg) {
    ( reinterpret_cast<SelectFile*>(arg) )->selectFile();
}

void* SelectFile::selectFile() {
	Record tempRec;
	ComparisonEngine comp;
	// assume that this file is all set up, it has been opened and is ready to go
	while( inFile->GetNext(tempRec) ){
		if(comp.Compare(&tempRec, literal, selOp)){
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

// applies that CNF to every tuple that comes through the pipe, and every
// tuple that is accepted is stuffed into the output pipe
SelectPipe::SelectPipe() : inPipe(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr) { }

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void SelectPipe::WaitUntilDone () { 
	pthread_join (thread, nullptr);
}

void SelectPipe::Use_n_Pages (int n) { 

}

void* SelectPipe::thread_wrapper (void* arg) {
    ( reinterpret_cast<SelectPipe*>(arg) )->selectPipe();
}

void* SelectPipe::selectPipe() {
	Record tempRec;
	ComparisonEngine comp;
	while( inPipe->Remove(&tempRec) ){
		if(comp.Compare(&tempRec, literal, selOp)){
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

// Project takes an input pipe and an output pipe as input. It also takes an array of
// integers keepMe as well as the number of attributes for the records coming through the
// input pipe and the number of attributes to keep from those input records. The array of
// integers tells Project which attributes to keep from the input records, and which order
// to put them in.
Project::Project() : inPipe(nullptr), outPipe(nullptr), keepMe(nullptr), numAttsInput(0), numAttsOutput(0) { }

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	numAttsOutput = numAttsOutput;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void Project::WaitUntilDone () {
	pthread_join (thread, nullptr);
}

void Project::Use_n_Pages (int n) {

}

void* Project::thread_wrapper (void* arg) {
    ( reinterpret_cast<Project*>(arg) )->project();
}

void* Project::project() {
	Record tempRec;
	while( inPipe->Remove(&tempRec) ){
		//Project (int *attsToKeep, int numAttsToKeep, int numAttsNow);
		tempRec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&tempRec);		
	}	
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

// Join takes two input pipes, an output pipe, and a CNF, and joins all of the records from
// the two pipes according to that CNF. Join should use a BigQ to store all of the tuples
// coming from the left input pipe, and a second BigQ for the right input pipe, and then
// perform a merge in order to join the two input pipes. You’ll create the OrderMakers
// for the two BigQ’s using the CNF (the function GetSortOrders will be used to create
// the OrderMakers). If you can’t get an appropriate pair of OrderMakers because the
// CNF can’t be implemented using a sort-merge join (due to the fact it does not have an
// equality check) then your Join operation should default to a block-nested loops join
Join::Join() : inPipeL(nullptr), inPipeR(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr), outputL(nullptr), outputR(nullptr), bqL(nullptr), bqR(nullptr) { }

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void Join::WaitUntilDone (){
	pthread_join (thread, nullptr);

}

void Join::Use_n_Pages (int n){

}

void* Join::thread_wrapper (void* arg) {
    ( reinterpret_cast<Join*>(arg) )->join();
}

void* Join::join(){
	Record tempRecL, tempRecR, joinedRec;
	ComparisonEngine comp;
	int buffsz = 100;
	int runlen = 10;
	OrderMaker sortorderL, sortorderR;
	if( 0 == selOp->GetSortOrders (sortorderL, sortorderR) ){
		// block-nested loops join
	}
	outputL = new Pipe(buffsz);
	outputR = new Pipe(buffsz);
	bqL = new BigQ(*inPipeL, *outputL, sortorderL, runlen);
	bqR = new BigQ(*inPipeR, *outputR, sortorderR, runlen);
	while( outputL->Remove(&tempRecL) && outputR->Remove(&tempRecR) ){
		if( comp.Compare(&tempRecL, &tempRecR, literal, selOp)){
	//???	joinedRec.MergeRecords (&tempRecL, &tempRecR, int numAttsLeft, int numAttsRight, int *attsToKeep, int numAttsToKeep, int startOfRight);
			outPipe->Insert(&joinedRec);		
		}		
	}	
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

// DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
// tuples coming through the input pipe, and does a duplicate removal. That is, everything
// that somes through the output pipe will be distinct. It will use the BigQ class to do the
// duplicate removal. The OrderMaker that will be used by the BigQ (which you’ll need
// to write some code to create) will simply list all of the attributes from the input tuples.
DuplicateRemoval::DuplicateRemoval() : inPipe(nullptr), outPipe(nullptr), mySchema(nullptr), output(nullptr), bq(nullptr) { }

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema; 
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void DuplicateRemoval::WaitUntilDone (){
	pthread_join (thread, nullptr);
}

void DuplicateRemoval::Use_n_Pages (int n){

}

void* DuplicateRemoval::thread_wrapper (void* arg) {
    ( reinterpret_cast<DuplicateRemoval*>(arg) )->duplicateRemoval();
}

void* DuplicateRemoval::duplicateRemoval(){
	Record tempRec;
	//last unduplicate record
	Record lastRec;
	OrderMaker DRorder(mySchema);
	ComparisonEngine comp;
	int buffsz = 100;
	int runlen = 10;	
	output = new Pipe(buffsz);	
	bq = new BigQ(*inPipe, *output, DRorder, runlen);
	//initial the first lastRec
	if(output->Remove(&tempRec)){
		lastRec	= tempRec;
		outPipe->Insert(&tempRec);
	}
	while( output->Remove(&tempRec) ){
		if( 0 == comp.Compare(&tempRec, &lastRec, &DRorder)){
			//pipe Insert will consume tempRec, have to save it before Insert
			lastRec	= tempRec;
			outPipe->Insert(&tempRec);
		}					
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);
}

// Sum computes the SUM SQL aggregate function over the input pipe, and puts a single
// tuple into the output pipe that has the sum. The function over each tuple that is
// summed is stored in an instance of the Function class that is also passed to Sum as an
// argument
Sum::Sum() : inPipe(nullptr), outPipe(nullptr), computeMe(nullptr) { }

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe; 
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void Sum::WaitUntilDone (){
	pthread_join (thread, nullptr);
}

void Sum::Use_n_Pages (int n){
}

void* Sum::thread_wrapper (void* arg) {
    ( reinterpret_cast<Sum*>(arg) )->sum();
}

void* Sum::sum(){
	// Type Apply (Record &toMe, int &intResult, double &doubleResult);
	Record tempRec;
	int intResult;
	double doubleResult;
	string result;
	struct Attribute att;
	att.name = strdup("SUM");
	if(inPipe->Remove(&tempRec)){
		att.myType = computeMe->Apply(tempRec, intResult, doubleResult);
		while( inPipe->Remove(&tempRec) ){		
			computeMe->Apply(tempRec, intResult, doubleResult);		
		}			
		switch(att.myType){
			case Int:
				//result.to_string(intResult);		
				break;
			case Double: 
				//result.to_string(doubleResult);
				break;				
		}
		Schema sumSchema("tempSchema", 1, &att);
		tempRec.ComposeRecord (&sumSchema, result.c_str());
		outPipe->Insert(&tempRec);		
	}
	outPipe->ShutDown();
	pthread_exit(nullptr);	
}

// GroupBy is a lot like Sum, except that it does grouping, and then puts one sum into the
// output pipe for each group. Every tuple put into the output pipe has a sum as the first
// attribute, followed by the values for each of the grouping attributes as the remainder of
// the attributes. The grouping is specified using an instance of the OrderMaker class that
// is passed in. The sum to compute is given in an instance of the Function class.
GroupBy::GroupBy() : inPipe(nullptr), outPipe(nullptr), output(nullptr), bq(nullptr), groupAtts(nullptr), computeMe(nullptr) { }

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void GroupBy::WaitUntilDone (){
	pthread_join (thread, nullptr);
}

void GroupBy::Use_n_Pages (int n){
}

void* GroupBy::thread_wrapper (void* arg) {
    ( reinterpret_cast<GroupBy*>(arg) )->groupBy();
}

void* GroupBy::groupBy(){
	Record tempRec, lastRec;
	ComparisonEngine comp;

	int intResult = 0;
	double doubleResult = 0;
	string result;

	int buffsz = 100;
	int runlen = 10;	
	output = new Pipe(buffsz);	
	bq = new BigQ(*inPipe, *output, *groupAtts, runlen);

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

// WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to write
// text version of the output records to the file.
WriteOut::WriteOut() : inPipe(nullptr), outFile(nullptr), mySchema(nullptr){ }

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create (&thread, nullptr, &thread_wrapper, this); 
}

void WriteOut::WaitUntilDone (){
	pthread_join (thread, nullptr);
}

void WriteOut::Use_n_Pages (int n){
}

void* WriteOut::thread_wrapper (void* arg) {
    ( reinterpret_cast<WriteOut*>(arg) )->writeOut();
}

void* WriteOut::writeOut(){
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