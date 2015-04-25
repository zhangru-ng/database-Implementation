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
	bool loaded;
	DBFile dbf;	// corresponding DBFile
	std::string dbf_path;
	Schema sch;	// corresponding schema
	TableInfo() : loaded(false) { }
	TableInfo(TableInfo &&rhs);
};

class Tables {
public:
	static string tblPrefix_;
	static string dbfPrefix_;
	static string tblName_[8];
	static string catalogPath_;
	std::unordered_map<std::string, TableInfo> tableInfo;	// hash table for relation name and relation infomation

	void Create(const char *tableName, struct AttList *attsList);
	void Create(const char *tableName, struct AttList *attsList, struct NameList *sortAtts);
	void CreateAll();
	void Load(const char *tableName, std::string fileName);
	void LoadAll();
	void Drop(const char *tableName);
	void Store(std::string resultName, Schema &schema);
	void UpdateStats(const char *tableName, Statistics &stat);
	void Clear();
	void Print() const;
};

#endif

