#include "DBFile.h"


DBFile::DBFile () : myInernalPoniter(NULL) {

}

DBFile::~DBFile () {
	delete myInernalPoniter;
}

bool DBFile::AssertInit(){
	if(myInernalPoniter != NULL){
		return true;
	}else{
		cerr << "ERROR: DBFile is not initialized!";
		exit(1);
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
	if(AssertInit()){
		return myInernalPoniter->Create(f_path, f_type, startup);
	}
}

int DBFile::Open (const char *f_path) {
	//create the associated text file
	string header = f_path;
	header += ".header";			
	//open the exist associate file
	ifstream metafile;
	metafile.open(header.c_str());
	if ( metafile.is_open() == false ){
		cerr << "Can't open associated file for " << f_path << "\n";
		return 0;
	}else{
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
		if(AssertInit()){
			return myInernalPoniter->Open(f_path);	
		}
	}
	
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	//call corresponding Load
	if(AssertInit()){
		myInernalPoniter -> Load(f_schema, loadpath);
	}
}

void DBFile::MoveFirst () {
	//call corresponding MoveFirst
	if(AssertInit()){
		myInernalPoniter -> MoveFirst();
	}
}

int DBFile::Close () {
	if(AssertInit()){
		//call corresponding Close
		return myInernalPoniter -> Close();
	}
}

void DBFile::Add (Record &rec) {
	//call corresponding Add
	if(AssertInit()){
		myInernalPoniter -> Add(rec);
	}
}

int DBFile::GetNext (Record &fetchme){
	//call corresponding GetNext version 1	
	if(AssertInit()){
		return myInernalPoniter -> GetNext(fetchme);
	}
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	//call corresponding GetNext version 2
	if(AssertInit()){
		return myInernalPoniter -> GetNext(fetchme, cnf, literal);
	}
}
