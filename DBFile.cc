#include "DBFile.h"


DBFile::DBFile () : myInternalPointer(nullptr) {

}

//check if the internal pointer is initialize
bool DBFile::AssertInit(){
	if(myInternalPointer != nullptr){
		return true;
	}else{
		//if the internal pointer is not initialize, all operation on this instant will cause serious problem
		cerr << "ERROR: DBFile is not initialized!\n";
		exit(1);
	}
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	switch(f_type){
		case heap: 
			myInternalPointer.reset(new HeapDBFile());
			startup = nullptr;
			break;
		case sorted: 
			myInternalPointer.reset(new SortedDBFile());
			break;
		// case tree: 
		//	myInternalPointer.reset(new BtreeDBFile());
		//	break;
		default:
			cerr << "ERROR: Can't create DBFile, File type doesn't exist!\n";
			return 0;
	}
	if(AssertInit()){
		return myInternalPointer->Create(f_path, f_type, startup);
	}
}

int DBFile::Open (const char *f_path) {
	//create the associated text file
	string header = f_path;
	header += ".header";			
	//open the exist associate file
	ifstream metafile(header.c_str());
	if ( false == metafile.is_open() ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	int type;
	//read file type from associate file
	metafile >> type;
	//check file type and create corresponding DBFile instance
	switch(static_cast<fType>(type)){
		case heap:
			myInternalPointer.reset(new HeapDBFile());
			break;
		case sorted:
			myInternalPointer.reset(new SortedDBFile());
			break;
		// case tree:
		//  myInternalPointer.reset(new BtreeDBFile());
		// 	break;
		default:
			cerr << "ERROR: Can't open DBFile, File type doesn't exist!\n";
			return 0;
	}	
	if(AssertInit()){
		return myInternalPointer->Open(f_path);	
	}

	
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	//call corresponding Load
	if(AssertInit()){
		myInternalPointer -> Load(f_schema, loadpath);
	}
}

void DBFile::MoveFirst () {
	//call corresponding MoveFirst
	if(AssertInit()){
		myInternalPointer -> MoveFirst();
	}
}

int DBFile::Close () {
	// if internal poi
	if(myInternalPointer != nullptr){
		return myInternalPointer -> Close();
	}else{
		return 1;
	}
}

void DBFile::Add (Record &rec) {
	//call corresponding Add
	if(AssertInit()){
		myInternalPointer -> Add(rec);
	}
}

int DBFile::GetNext (Record &fetchme){
	//call corresponding GetNext version 1	
	if(AssertInit()){
		return myInternalPointer -> GetNext(fetchme);
	}
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//call corresponding GetNext version 2
	if(AssertInit()){
		return myInternalPointer -> GetNext(fetchme, cnf, literal);
	}
}
