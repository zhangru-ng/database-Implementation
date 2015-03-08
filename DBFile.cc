#include "DBFile.h"


DBFile::DBFile () : myInernalPoniter(NULL) {

}

DBFile::~DBFile () {
	
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	switch(f_type){
		case heap: 
			myInernalPoniter = new HeapDBFile();
			startup = NULL;
			break;
		case sorted: 
			myInernalPoniter = new SortedDBFile();
			break;
	//case tree: 
	//	myInernalPoniter = new TreeDBFile(); 
	//	break;
		default:
			cerr << "ERROR: Can't create DBFile, File type doesn't exist!";
			return 0;
	}
	if(myInernalPoniter != NULL){
		return myInernalPoniter->Create(f_path, f_type, startup);
	}else{
		cerr << "internal class pointer myInernalPoniter is NULL";
		return 0;
	}	
}

int DBFile::Open (const char *f_path) {
	//create the associated text file
	string header = f_path;
	header += ".header";			
	//open the exist associate file
	ifstream metafile(header.c_str());
	if ( metafile.is_open() == false ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	int type;
	//read file type from associate file
	metafile >> type;
	metafile.close();
	//check file type and create corresponding DBFile instance
	switch((fType)type){
		case heap:
			myInernalPoniter = new HeapDBFile();
			break;
		case sorted:
			myInernalPoniter = new SortedDBFile();
			break;
		case tree:
			// myInernalPoniter = new TreeDBFile(); 
			break;
		default:
			cerr << "ERROR: Can't open DBFile, File type doesn't exist!";
			return 0;
	}	
	if(myInernalPoniter != NULL){
		return myInernalPoniter->Open(f_path);	
	}else{
		cerr << "internal class pointer myInernalPoniter is NULL";
		return 0;
	}
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	//call corresponding Load
	myInernalPoniter -> Load(f_schema, loadpath);
}

void DBFile::MoveFirst () {
	//call corresponding MoveFirst
	myInernalPoniter -> MoveFirst();
}

int DBFile::Close () {
	if(myInernalPoniter != NULL){
		//call corresponding Close
		return myInernalPoniter -> Close();
		delete myInernalPoniter;
	}else{
		return 0;
	}
}

void DBFile::Add (Record &rec) {
	//call corresponding Add
	myInernalPoniter -> Add(rec);
}

int DBFile::GetNext (Record &fetchme){
	//call corresponding GetNext version 1
	return myInernalPoniter -> GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//call corresponding GetNext version 2
	return myInernalPoniter -> GetNext(fetchme, cnf, literal);
}
