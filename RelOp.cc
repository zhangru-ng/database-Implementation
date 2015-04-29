#include "RelOp.h"


void RelationalOp::WaitUntilDone () {
	 WaitForInternalThreadToExit();
}

void RelationalOp::Use_n_Pages (int n) {
	runlen = n;
}

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

// performs a scan of the underlying file, and for every tuple accepted by the CNF, 
// it stuffs the tuple into the pipe as output
void* SelectFile::InternalThreadEntry() {
	Record tempRec;
	ComparisonEngine comp;
	// assume that this file is all set up, it has been opened and is ready to go
	inFile->MoveFirst();
	while (1 == inFile->GetNext(tempRec)) {
		//if the record is accepted by input CNF
		if (1 == comp.Compare(&tempRec, literal, selOp)) {
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
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

// applies that CNF to every tuple that comes through the pipe, and every
// tuple that is accepted is stuffed into the output pipe
void* SelectPipe::InternalThreadEntry () {
	Record tempRec;
	ComparisonEngine comp;
	while (1 == inPipe->Remove(&tempRec)) {
		//if the record is accepted by input CNF
		if (1 == comp.Compare(&tempRec, literal, selOp)) {
			outPipe->Insert(&tempRec);
		}
	}
	outPipe->ShutDown();
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

// Project takes an input pipe and an output pipe as input. It also takes an array of integers keepMe 
// as well as the number of attributes for the records coming through the input pipe and the number of 
// attributes to keep from those input records. The array of integers tells Project which attributes to 
// keep from the input records, and which order to put them in.
void* Project::InternalThreadEntry () {
	Record tempRec;
	while (1 == inPipe->Remove(&tempRec)) {
		//Project (int *attsToKeep, int numAttsToKeep, int numAttsNow);
		tempRec.Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(&tempRec);		
	}	
	outPipe->ShutDown();
}
/**************************************Project***********************************************/


/***************************************Join*************************************************/
Join::Join() : inPipeL(nullptr), inPipeR(nullptr), outPipe(nullptr), selOp(nullptr), literal(nullptr), numAttsLeft(0), numAttsRight(0), numAttsToKeep(0), attsToKeep(nullptr)  { }

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	StartInternalThread();
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
	if (nullptr == selOp) {
		NestedLoopJoin(PRODUCT);
	} else {
		if(!selOp->GetSortOrders (sortorderL, sortorderR)) {
			NestedLoopJoin(JOIN);
		} else {
			SortMergeJoin(sortorderL, sortorderR);
		}	 	
	}	
	outPipe->ShutDown();
}

void Join::NestedLoopJoin(int flag) {
	Record tempRecL, tempRecR, joinRec;
	vector<Record> leftRecords;	
	bool fitInMemory = true;
	//create the temporary file for external block nested join
	char lfname[80] = "dbfile/temp/JLtemp_XXXXXX";
	mkstemp(lfname);
	DBFile leftFile;
	leftFile.Create(lfname, heap, nullptr);

	if (inPipeL->Remove(&tempRecL) && inPipeR->Remove(&tempRecR)) {
		//initial parameters for MergeRecord
		{
			numAttsLeft = tempRecL.GetNumAtts();
			numAttsRight = tempRecR.GetNumAtts();		
			numAttsToKeep = numAttsLeft + numAttsRight;
			attsToKeep = new int[numAttsToKeep];
			for (int i = 0; i < numAttsToKeep; ++i) {
				if (i >= numAttsLeft) {
					//initial right relation
					attsToKeep[i] = i - numAttsLeft;
					continue;
				}
				attsToKeep[i] = i;
			}
		}
		//maximum internal memory join can use
		long maxSize = (2 * runlen * PAGE_SIZE) / tempRecL.Size();
		leftRecords.reserve(maxSize);
		int curSize = 1;
		leftRecords.push_back(std::move(tempRecL));
			
		while (curSize < maxSize && inPipeL->Remove(&tempRecL)) {
			leftRecords.push_back(std::move(tempRecL));
			curSize++;
		}
		// if the size of smaller relation greater than max size, external join is needed
		if (curSize == maxSize) {
			fitInMemory = false;
			for (auto &rec : leftRecords) {
				leftFile.Add(rec);
			}
			while(inPipeL->Remove(&tempRecL)) {
				leftFile.Add(tempRecL);
			}			
		}	
		if (true == fitInMemory) {
			cout << "Relations not fit in memory, perform in Memory join" << endl; 
			InMemoryJoin(leftRecords, inPipeR, tempRecR, flag);					
		} else {		
			cout << "Relations not fit in memory, perform external join" << endl; 
			InFileJoin(leftFile, inPipeR, tempRecR, flag);
		}		
	}			
	//clean up temporary allocation and file
	delete[] attsToKeep;
	leftFile.Close();
	remove(lfname);
}

// nested loop scan the relation and join records
void Join::InMemoryJoin (vector<Record> &leftRecords, Pipe *inPipeR, Record &tempRecR, int flag) {
	JoinRecInMemory(leftRecords, tempRecR, flag);
	Record right;
	// nested loop scan
	while(inPipeR->Remove(&right)) {
		JoinRecInMemory(leftRecords, right, flag);
	}	
}

void Join::JoinRecInMemory(vector<Record> &leftRecords, Record &right, int flag) {
	Record joinRec;		
	for (auto &left : leftRecords) {
		if(PRODUCT == flag || comp.Compare(&left, &right, literal, selOp)){
			joinRec.MergeRecords (&left, &right, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
			outPipe->Insert(&joinRec);
		}	
	}
}

void Join::InFileJoin (DBFile &file, Pipe *inPipeR, Record &tempRecR, int flag) {
	Record left, right;
	JoinRecInFile(file, tempRecR, flag);
	// nested loop scan
	while (inPipeR->Remove(&right)) {
		JoinRecInFile(file, right, flag);
	}
}

void Join::JoinRecInFile (DBFile &file, Record &right, int flag) {
	Record left;
	Record joinRec;
	file.MoveFirst();
	while (file.GetNext(left)) {
		if (PRODUCT == flag || comp.Compare(&left, &right, literal, selOp)) {
			joinRec.MergeRecords (&left, &right, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
			outPipe->Insert(&joinRec);
		}	
	}	
}

//sort-merge join algorithm
void Join::SortMergeJoin (OrderMaker &sortorderL, OrderMaker &sortorderR) {
	Record tempRecL, tempRecR;
	Pipe *outputL = new Pipe(buffsz);
	Pipe *outputR = new Pipe(buffsz);
	BigQ *bqL = new BigQ(*inPipeL, *outputL, sortorderL, runlen/2);
	BigQ *bqR = new BigQ(*inPipeR, *outputR, sortorderR, runlen/2);
	//if both the output pipes have at least one record
	if (outputL->Remove(&tempRecL) && outputR->Remove(&tempRecR)) {
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
		bool leftNotEnd = true;
		bool rightNotEnd = true;
		Record joinRec;
		while(leftNotEnd && rightNotEnd) {
			// if the left record is less than right record
			if (comp.Compare(&tempRecL, &sortorderL, &tempRecR, &sortorderR) < 0) {
				//get next record in left output pipe for next comparion, if no record left then break
				leftNotEnd = outputL->Remove(&tempRecL);
			}
			// if the left record is greater than right record
			else if (comp.Compare(&tempRecL, &sortorderL, &tempRecR, &sortorderR) > 0) {
				//get next record in right output pipe for next comparion, if no record left then break
				rightNotEnd = outputR->Remove(&tempRecR);
			}
			// if left record equal to right record
			else {
				vector<Record> leftRecords;
				vector<Record> rightRecords;
				leftRecords.push_back(std::move(tempRecL));
				rightRecords.push_back(std::move(tempRecR));
				//collect all equal records in left pipe
				leftNotEnd = outputL->Remove(&tempRecL);
				while (leftNotEnd && 0 == comp.Compare(&tempRecL, &sortorderL, &rightRecords[0], &sortorderR)){
					leftRecords.push_back(std::move(tempRecL));
					leftNotEnd = outputL->Remove(&tempRecL);
				}
				//collect all equal records in right pipe
				rightNotEnd = outputR->Remove(&tempRecR);
				while (rightNotEnd && 0 == comp.Compare(&leftRecords[0], &sortorderL, &tempRecR, &sortorderR)) {
					rightRecords.push_back(std::move(tempRecR));
					rightNotEnd = outputR->Remove(&tempRecR);
				}
				//merge all matched records
				for (auto itL = leftRecords.begin(); itL != leftRecords.end(); ++itL) {
					for (auto itR = rightRecords.begin(); itR != rightRecords.end(); ++itR) {
						joinRec.MergeRecords (&(*itL), &(*itR), numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
						outPipe->Insert(&joinRec);
					}
				}
			}
		}
		// while(outputL->Remove(&tempRecL));
		// while(outputR->Remove(&tempRecR));
	}
	// don't need to spend time taking out the unmatched record?
	outputL->ShutDown();
	outputR->ShutDown();
	delete bqL;
	delete bqR;
	delete outputL;
	delete outputR;
	delete[] attsToKeep;
}
/***************************************Join*************************************************/

// @tested
/**********************************DuplicateRemoval******************************************/
DuplicateRemoval::DuplicateRemoval() : inPipe(nullptr), outPipe(nullptr), mySchema(nullptr) { }

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema; 
	StartInternalThread();
}

// DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
// tuples coming through the input pipe, and does a duplicate removal. That is, everything
// that somes through the output pipe will be distinct. It will use the BigQ class to do the
// duplicate removal. The OrderMaker that will be used by the BigQ (which you’ll need
// to write some code to create) will simply list all of the attributes from the input tuples.
void* DuplicateRemoval::InternalThreadEntry () {
	Record tempRec;
	//last unduplicate record
	Record lastRec;
	OrderMaker DRorder(mySchema);
	ComparisonEngine comp;
	Pipe output(buffsz);	
	BigQ bq(*inPipe, output, DRorder, runlen);
	//initial the first lastRec
	if (output.Remove(&tempRec)) {
		lastRec = tempRec;
		outPipe->Insert(&tempRec);
	}
	while (output.Remove(&tempRec)) {
		if (0 != comp.Compare(&tempRec, &lastRec, &DRorder)) {
			//pipe Insert will consume tempRec, have to save it before Insert
			lastRec = tempRec;
			outPipe->Insert(&tempRec);
		}					
	}
	//shut down output pipe
	outPipe->ShutDown();
}
/**********************************DuplicateRemoval******************************************/

// @tested
/***************************************Sum**************************************************/
Sum::Sum() : inPipe(nullptr), outPipe(nullptr), computeMe(nullptr), intSum(0), doubleSum(0) { }

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe; 
	StartInternalThread();
}

// Sum computes the SUM SQL aggregate function over the input pipe, and puts a single tuple
// into the output pipe that has the sum. The function over each tuple that is summed is 
// stored in an instance of the Function class that is also passed to Sum as an argument
void* Sum::InternalThreadEntry () {
	// Type Apply (Record &toMe, int &intResult, double &doubleResult);	
	Record tempRec;
	string result;
	struct Attribute att;
	if (inPipe->Remove(&tempRec)) {
		att.name = "SUM";
		att.myType = computeMe->Apply(tempRec, intSum, doubleSum);	
		//check attribute type to decide following step
		switch (att.myType) {
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
	}
	//shut down output pipe
	outPipe->ShutDown();	
}

string Sum::IntSum () {
	int intResult;
	double dummy;
	Record tempRec;
	while (inPipe->Remove(&tempRec)) {		
		computeMe->Apply(tempRec, intResult, dummy);		
		intSum += intResult;
	}		
	return to_string(intSum);		
}


string Sum::DoubleSum () {
	double doubleResult;
	int dummy;
	Record tempRec;
	while (inPipe->Remove(&tempRec)) {		
		computeMe->Apply(tempRec, dummy, doubleResult);		
		doubleSum += doubleResult;
	}		
	return to_string(doubleSum);
}
/***************************************Sum**************************************************/


/*************************************GroupBy************************************************/
GroupBy::GroupBy() : inPipe(nullptr), outPipe(nullptr), groupAtts(nullptr), computeMe(nullptr) { }

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	StartInternalThread();
}

// GroupBy is a lot like Sum, except that it does grouping, and then puts one sum into the
// output pipe for each group. Every tuple put into the output pipe has a sum as the first
// attribute, followed by the values for each of the grouping attributes as the remainder of
// the attributes. The grouping is specified using an instance of the OrderMaker class that
// is passed in. The sum to compute is given in an instance of the Function class.
void* GroupBy::InternalThreadEntry () {
	Record tempRec, lastRec;
	// initial parameters for MergeRecord
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

	if (output.Remove(&tempRec)) {
		//initial the aggregation attribute
		struct Attribute att;
		att.name = "SUM";
		att.myType = computeMe->Apply(tempRec, intSum, doubleSum);
		Schema sumSchema("tempSchema", 1, &att);
		int numAtts = tempRec.GetNumAtts();
		lastRec	= tempRec;
		switch (att.myType) {
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
	//clean up
	delete[] attsToKeep;
	outPipe->ShutDown();
}

void GroupBy::GroupInt (Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *attsToKeep, Schema &sumSchema) {
	Record tempRec, sumnRecord, groupRec;
	ComparisonEngine comp;
	int intResult;
	double dummy;
	string result;
	while (output.Remove(&tempRec)) {
		//if the record is in this group
		if (0 == comp.Compare(&tempRec, &lastRec, groupAtts)) {
			//compute the aggregation
			computeMe->Apply(tempRec, intResult, dummy);
			intSum += intResult;
		} else {
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

void GroupBy::GroupDouble (Pipe &output, Record &lastRec, int numAtts, int numGroupAtts, int *attsToKeep, Schema &sumSchema) {
	Record tempRec, sumnRecord, groupRec;
	ComparisonEngine comp;
	int dummy;
	double doubleResult;
	string result;
	while (output.Remove(&tempRec)) {
		//if the record is in this group
		if (0 == comp.Compare(&tempRec, &lastRec, groupAtts)) {
			//compute the aggregation
			computeMe->Apply(tempRec, dummy, doubleResult);
			doubleSum += doubleResult;
		} else {
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

void WriteOut::Run (Pipe &inPipe, Schema &mySchema, int outputMode) {
	this->inPipe = &inPipe;
	this->mySchema = &mySchema;
	this->outputMode = outputMode;
	StartInternalThread();
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema, int outputMode) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	this->outputMode = outputMode;
	StartInternalThread();
}

// WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to write
// text version of the output records to the file.
void* WriteOut::InternalThreadEntry() {
	Record tempRec;	
	if (STDOUT_== outputMode) {
		while (inPipe->Remove(&tempRec)) {
			tempRec.Print(mySchema);			
		}	
	} else if (OUTFILE_ == outputMode) {
		int n = mySchema->GetNumAtts();
		Attribute *atts = mySchema->GetAtts();
		while (inPipe->Remove(&tempRec)) {
			cout << "write" << endl;
			// loop through all of the attributes
			for (int i = 0; i < n; i++) {
				// print the attribute name
				int pointer = ((int *) tempRec.bits)[i + 1];
				// print out the contents depend on the type
				if (atts[i].myType == Int) {
					int *myInt = (int *) &(tempRec.bits[pointer]);
					fprintf(outFile, "%d", *myInt);
					fprintf(outFile, "|");
				} else if (atts[i].myType == Double) {
					double *myDouble = (double *) &(tempRec.bits[pointer]);
					fprintf(outFile, "%f", *myDouble);
					fprintf(outFile, "|");
				} else if (atts[i].myType == String) {
					char *myString = (char *) &(tempRec.bits[pointer]);
					fprintf(outFile, "%s", myString);
					fprintf(outFile, "|");
				} 
			}
			fprintf(outFile, "\n");	
		}
		fclose(outFile);
	}
}
/*************************************WriteOut***********************************************/


/*
 *Discard 2015.03.20
 *The performance difference is little
 *but the code is much longer
 */
/*********************************BlockNestedJoin********************************************
void Join::WriteToFile (vector<Record> &run, File &file) {
	Page tempPage;
	Record tempRec;
	off_t tempIndex;
	off_t length = file.GetLength();
	if (0 == length) {
		tempIndex = 0;
	} else {
		tempIndex = length - 2;
		file.GetPage(&tempPage, tempIndex);
	}
	for (auto it = run.begin();it != run.end(); ++it) {
		tempRec.Consume(&(*it));
		if (0 == tempPage.Append(&tempRec)) {
			//if the page is full, create a new page
			file.AddPage(&tempPage, tempIndex);  
			tempPage.EmptyItOut();
			tempPage.Append(&tempRec);
			tempIndex++;
		}
	} 
	file.AddPage(&tempPage, tempIndex);  
}


//external block nested loop join
void Join::JoinRecInFile (File &outter, File &inner, int smaller) {
	Record tempRecL, tempRecR, joinRec;
	Page tempPage;
	Page block[2 * runlen - 1];

	off_t outterIndex = 0;
	off_t outterLength = outter.GetLength() - 2;
	off_t innerLength = inner.GetLength() - 2;
	// pointer-to-member-function, typedef right after class Join
	JoinRecFn JoinRec;
	size_t count = 0;
	//check which relation is smaller, use corresponding join function
	if (LEFT == smaller) {
		JoinRec = &Join::JoinRecordLR;
	} else {
		JoinRec = &Join::JoinRecordRL;
	}	
	while (outterIndex <= outterLength) {
		//fill M-1 block of records into memory
		int size = FillBlock(outter, block, outterIndex);	
		for (int i = 0; i < size; i++) {
			//for each record in outter block
			while (block[i].GetFirst(&tempRecL)) {
				//scan the whole inner relation to find match records
				for (off_t innerIndex = 0; innerIndex <= innerLength; ++innerIndex) {
					inner.GetPage(&tempPage, innerIndex);
					while (tempPage.GetFirst(&tempRecR)) {
						(this->*JoinRec)(tempRecL, tempRecR);
						// count++;
						// if(count % 1000000 == 0){
						// 	cerr << count << "\n";
						// }
					}																									
				}
			}
		}				
	}		
}

// Fill M-1 blocks for the outter loop of block nested join
int Join::FillBlock (File &file, Page block[], off_t &index){
	Record tempRec;
	Page tempPage;
	// puump records of the smaller relation into M-1 pages
	int n = 2 * runlen - 1;
	off_t length = file.GetLength() - 2; 
	for (int i = 0; i < n; ++i) {
		// if current index less than file lenght, fill the blocks
		if (index <= length) {
			file.GetPage(&block[i], index);			
			++index;			
		}
		// otherwise return number of blocks that has been filled
		else {
			return i;
		}
	}
	return n;
}
**********************************BlockNestedJoin********************************************/
