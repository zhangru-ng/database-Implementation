
#include "SortedDBFile.h"

SortedDBFile::SortedDBFile () {

}

SortedDBFile::~SortedDBFile () {

}

int SortedDBFile::Create (char *f_path, fType f_type, void *startup) {
}

void SortedDBFile::Load (Schema &f_schema, char *loadpath) {
}

int SortedDBFile::Open (char *f_path) {
}

void SortedDBFile::MoveFirst () {
}

int SortedDBFile::Close () {
}

void SortedDBFile::Add (Record &rec) {
}

int SortedDBFile::GetNext (Record &fetchme) {
	return 0;
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
