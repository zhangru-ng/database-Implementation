#ifndef TABLES_H
#define TABLES_H

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "DBFile.h"
#include "Defs.h"
#include <string>


enum TABLE { REGION = 0, NATION, CUSTOMER, PART, PARTSUPP, SUPPLIER, ORDER, LINEITEM };


struct TableInfo{
	DBFile dbf;	// corresponding DBFile
	Schema sch;	// corresponding schema
	TableInfo() = default;
	TableInfo(TableInfo &&rhs) {
		dbf = std::move(rhs.dbf);
		sch = std::move(rhs.sch);
	}
};

class Tables {
	static string tblPrefix_;
	static string dbfPrefix_;
	static string tblName_[8];
	static string catalogPath_;
	std::unordered_map<std::string, TableInfo> &tableInfo;	// hash table for relation name and relation infomation
public:
	Tables(std::unordered_map<std::string, TableInfo> &tbl);
	void Create(const char *tableName, struct AttList *attsList);
	void Create(const char *tableName, struct AttList *attsList, struct NameList *sortAtts);
	void Load(const char *tableName, std::string fileName);
	void Drop(const char *tableName);
	void UpdateStats(const char *tableName, Statistics &stat);
};

#endif

