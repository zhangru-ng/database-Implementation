
#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () : myOrder(), runLength(0), curMode(Read), input(NULL), output(NULL), bq(NULL) {	

}

SortedDBFile::~SortedDBFile () {
	if(input != NULL){
		delete[] input;
	}
	if(output != NULL){
		delete[] output;
	}
	if(bq != NULL){
		delete[] bq;
	}
}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
	string header = f_path;
	//create the binary DBfile
	curFile.Open (0, f_path);
	
	//create the associated text file
	header += ".header";

	ofstream metafile(header.c_str ());
	if (!metafile.is_open()){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}

	metafile << "sorted" << endl;
	struct SortInfo* sip = reinterpret_cast<struct SortInfo*> (startup);
	myOrder = *(sip -> order);
	runLength = sip -> runlen;
	metafile.write (reinterpret_cast<const char *> (&myOrder), sizeof (OrderMaker));
	metafile << runLength << endl;

	metafile.close();
	return 1;
}

int SortedDBFile::Open (char *f_path) {
	string header = f_path;
	//open the exist associate file
	header += ".header";

	ifstream metafile(header.c_str ());
	if (!metafile.is_open ()){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}	
	
	metafile.seekg (7, metafile.beg);
	metafile.read (reinterpret_cast<char *> (&myOrder), sizeof (OrderMaker));
	metafile >> runLength;

	metafile.close ();
//	myOrder.Print ();
	curFile.Open (1, f_path);
	return 1;
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
	Record tempRec;
	FILE *tableFile = fopen (loadpath, "r");

	if(tableFile == NULL){
		cerr << "Can't open table file: " << loadpath << "\n";
	}

	if(curMode == Read){
		if( bq == NULL){
			input = new Pipe (buffsz);
			output = new Pipe (buffsz);
			bq = new BigQ (input, output, myOrder, runLength);
		}		
		curMode = Write;
	}
	
    while (tempRec.SuckNextRecord (&f_schema, tableFile) == 1) {
		input.Insert (&tempRec);
    }
    fclose(fp);
}

void SortedDBFile::Add (Record &rec) {
	if(curMode == Read){
		if( bq == NULL){
			input = new Pipe (buffsz);
			output = new Pipe (buffsz);
			bq = new BigQ (input, output, myOrder, runLength);
		}		
		curMode = Write;
	}

	input.Insert (&rec);
}

void SortedDBFile::MoveFirst () {

}

int SortedDBFile::Close () {
	curFile.Close ();
}



int SortedDBFile::GetNext (Record &fetchme) {

}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

}

void SortedDBFile::MergeSorted(){
	File tempFile;
	Page tempPage;
	Record tempRec1, tempRec2;

	ComparisonEngine comp;
	int tempIndex = 0;
	input.ShutDown ();

	while(tempPage.GetFirst(&tempRec2)){
		while(output.Remove(&tempRec)){
			if(comp.Compare (tempRec1,tempRec2, myOrder) < 0){

			}
		}
	}
		
}