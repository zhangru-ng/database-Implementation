#include "DBFile.h"


DBFile::DBFile () : myInernalPoniter(NULL) {

}

DBFile::~DBFile () {
	if(myInernalPoniter != NULL){
		delete myInernalPoniter;
	}
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
	return myInernalPoniter -> Create(f_path, f_type, startup);
}

int DBFile::Open (const char *f_path) {
	//create the associated text file
	string header = f_path;
	header += ".header";			
	//open the exist associate file
	ifstream metafile(header.c_str());
	if (!metafile.is_open()){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}
	string type;
	//read file type from associate file
	metafile >> type;
	metafile.close();
	//check file type and create corresponding DBFile instance
	if(type.compare("heap") == 0){
		myInernalPoniter = new HeapDBFile();
	}else if(type.compare("sorted") == 0){
		myInernalPoniter = new SortedDBFile();
	}
	// else if(type.compare("tree") == 0){
	// 	myInernalPoniter = new TreeDBFile(); 
	// }
	else{
		cerr << "ERROR: Can't open DBFile, File type doesn't exist!";
		return 0;
	}	
	return myInernalPoniter -> Open(f_path);	
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
	//call corresponding Close
	return myInernalPoniter -> Close();
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
