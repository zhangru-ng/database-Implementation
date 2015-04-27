#ifndef TABLES_H
#define TABLES_H

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
#include <string>

using std::ifstream;
using std::ofstream;

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
	std::string tblPrefix_;
	std::string dbfPrefix_;
	std::vector<std::string> tblName_;
	std::string catalogPath_;
	std::unordered_map<std::string, TableInfo> tableInfo;	// hash table for relation name and relation infomation
	Tables();
	void Create(const char *tableName, struct AttList *attsList);
	void Create(const char *tableName, struct AttList *attsList, struct NameList *sortAtts);
	void CreateAll();
	void Load(const char *tableName, std::string fileName);
	void LoadAll();
	void Drop(const char *tableName);
	void Store(std::string resultName, Schema &schema);
	void UpdateStats(const char *tableName, Statistics &stat);
	void AppendSchema(const char *tableName);
	void RenewSchema();
	void Read(std::string filename);
	void Write(std::string filename);
	void Clear();
	void Print() const;
};

#endif

