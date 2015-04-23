#include "Tables.h"

std::string Tables::tblPrefix_ = "/cise/homes/rui/Desktop/testfiles/10M_table/";
std::string Tables::dbfPrefix_ = "dbfile/";
std::string Tables::tblName_[8] = {"region", "nation", "customer", "part", "partsupp", "supplier", "order", "lineitem"};
std::string Tables::catalogPath_ = "catalog";

Tables::Tables(std::unordered_map<std::string, TableInfo> &tbl) : tableInfo(tbl) { }

void Tables::Create(const char* tableName, struct AttList *attsList) {
	TableInfo tblInfo;
	tblInfo.sch = std::move(Schema(catalogPath_.c_str(), tableName));
	string file_path(dbfPrefix_ + std::string(tableName)+".bin");
	tblInfo.dbf.Create(file_path.c_str(), heap, 0);	
	tableInfo.emplace(tableName, std::move(tblInfo));
}


void Tables::Create(const char *tableName, struct AttList *attsList, struct NameList *sortAtts) {
	TableInfo tblInfo;
	tblInfo.sch = std::move(Schema(catalogPath_.c_str(), tableName));
	string file_path(dbfPrefix_ + std::string(tableName)+".bin");
	OrderMaker om(sortAtts, tblInfo.sch);
	SortInfo si = { &om, 50 };
	tblInfo.dbf.Create(file_path.c_str(), sorted, &si);	
	tableInfo.emplace(tableName, std::move(tblInfo));
}

void Tables::Load(const char *tableName, std::string fileName) {
	TableInfo &tblInfo = tableInfo.at(tableName);
	string load_path(tblPrefix_ + std::string(tableName)+".tbl");
	tblInfo.dbf.Load (tblInfo.sch, load_path.c_str());	
}

void Tables::Drop(const char *tableName) {
	tableInfo.erase(tableName);
}

void Tables::UpdateStats(const char *tableName, Statistics &stat) {

}

