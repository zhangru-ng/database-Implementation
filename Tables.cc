#include "Tables.h"

std::string Tables::tblPrefix_ = "/cise/tmp/dbi_sp11/DATA/1G/";
std::string Tables::dbfPrefix_ = "dbfile/";
std::string Tables::tblName_[8] = {"region", "nation", "customer", "part", "partsupp", "supplier", "orders", "lineitem"};
std::string Tables::catalogPath_ = "catalog";



TableInfo::TableInfo(TableInfo &&rhs) {
	loaded = rhs.loaded;
	dbf = std::move(rhs.dbf);
	dbf_path = std::move(rhs.dbf_path);
	sch = std::move(rhs.sch);
}

void Tables::Create(const char* tableName, struct AttList *attsList) {
	cout << "Creating heap file for table " << tableName << endl;
	TableInfo tblInfo;
	tblInfo.sch = std::move(Schema(attsList));
	tblInfo.dbf_path = dbfPrefix_ + std::string(tableName)+ ".bin";
	tblInfo.dbf.Create(tblInfo.dbf_path.c_str(), heap, 0);	
	tblInfo.dbf.Close();
	tableInfo.emplace(tableName, std::move(tblInfo));
}


void Tables::Create(const char *tableName, struct AttList *attsList, struct NameList *sortAtts) {
	cout << "Creating sorted file for table " << tableName << endl;
	TableInfo tblInfo;
	tblInfo.sch = std::move(Schema(attsList));
	OrderMaker om(sortAtts, tblInfo.sch);
	SortInfo si = { &om, 50 };
	tblInfo.dbf_path = dbfPrefix_ + std::string(tableName) + ".bin";
	tblInfo.dbf.Create(tblInfo.dbf_path.c_str(), sorted, &si);	
	tblInfo.dbf.Close();
	tableInfo.emplace(tableName, std::move(tblInfo));
}

void Tables::CreateAll() {
	for(int i = 0; i< 8; ++i) {
		TableInfo tblInfo;
		tblInfo.sch = std::move(Schema(catalogPath_.c_str(), tblName_[i].c_str()));
		tblInfo.dbf_path = dbfPrefix_ + tblName_[i] + ".bin";
		tblInfo.dbf.Create(tblInfo.dbf_path.c_str(), heap, 0);	
		tblInfo.dbf.Close();
		tableInfo.emplace(tblName_[i], std::move(tblInfo));
	}
}

void Tables::Load(const char *tableName, std::string fileName) {
	cout << "Loading table " << tableName << " from " << fileName << endl;
	TableInfo &tblInfo = tableInfo.at(tableName);
	tblInfo.dbf.Open(tblInfo.dbf_path.c_str());
	string load_path(tblPrefix_ + std::string(tableName) + ".tbl");
	tblInfo.dbf.Load (tblInfo.sch, load_path.c_str());
	tblInfo.dbf.Close();
	tblInfo.loaded = true;
}

void Tables::LoadAll() {
	for(int i = 0; i< 8; ++i) {
		TableInfo &tblInfo = tableInfo.at(tblName_[i]);
		tblInfo.dbf.Open(tblInfo.dbf_path.c_str());
		string load_path(tblPrefix_ + tblName_[i] + ".tbl");
		tblInfo.dbf.Load (tblInfo.sch, load_path.c_str());
		tblInfo.dbf.Close();
		tblInfo.loaded = true;
	}
}

void Tables::Drop(const char *tableName) {
	char ch;				
	while (true) {
		cout << "Are you sure to delete table " << tableName << "? (y or n)" << endl;
		cin >> ch;
		if('y' == ch) {
			cout << "Deleting table " << tableName << endl;
			tableInfo.erase(tableName);
			break;
		} else if ('n' == ch) {
			break;
		} 
	}	
}

void Tables::Store(std::string resultName, Schema &schema) {
	TableInfo tblInfo;
	tblInfo.sch = std::move(schema);
	tblInfo.dbf_path = dbfPrefix_ + resultName + ".bin";
	tblInfo.dbf.Create(tblInfo.dbf_path.c_str(), heap, 0);	
	tableInfo.emplace(resultName, std::move(tblInfo));
}

void Tables::UpdateStats(const char *tableName, Statistics &stat) {

}

void Tables::Print() const {
	if (tableInfo.empty()) {
		cout << "Database has no table currently." << endl;
		return;
	}
	for (auto &t : tableInfo) {
		cout << "Name: " << t.first << " (" << (t.second.loaded == true ? "Loaded" : "Not loaded") << ")" << endl;
		cout << "Schema: " << endl;
		t.second.sch.Print();
		cout << endl;
	}

}

void Tables::Clear() {
	for (auto &t : tableInfo) {
		t.second.dbf.Close();
		remove(t.first.c_str());
	}
	tableInfo.clear();
}
