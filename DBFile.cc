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
	//convert file type string to enum
	fType f_type = StringToEnum(type);
	switch(f_type){
	case heap: 
		myInernalPoniter = new HeapDBFile();
		break;
	case sorted: 
		myInernalPoniter = new SortedDBFile();
		break;
	//case tree: 
	//	myInernalPoniter = new TreeDBFile(); 
	//	break;
	}
	return myInernalPoniter -> Open(f_path);	
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	myInernalPoniter -> Load(f_schema, loadpath);
}

void DBFile::MoveFirst () {
	myInernalPoniter -> MoveFirst();
}

int DBFile::Close () {
	return myInernalPoniter -> Close();
}

void DBFile::Add (Record &rec) {
	myInernalPoniter -> Add(rec);
}

int DBFile::GetNext (Record &fetchme){
	return myInernalPoniter -> GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return myInernalPoniter -> GetNext(fetchme, cnf, literal);
}

//convert string to enum to wrtie into a txt file
fType DBFile::StringToEnum(const string &type){
	if(type.compare("heap") == 0){
		return heap;
	}else if(type.compare("sorted") == 0){
		return sorted;
	}else{
		return tree;
	}	
}