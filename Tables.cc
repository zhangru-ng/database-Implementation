#include "Tables.h"

TableInfo::TableInfo(TableInfo &&rhs) {
	loaded = rhs.loaded;
	dbf = std::move(rhs.dbf);
	dbf_path = std::move(rhs.dbf_path);
	sch = std::move(rhs.sch);
}

Tables::Tables() {
	tblPrefix_ = "/cise/tmp/dbi_sp11/DATA/1G/";
	dbfPrefix_ = "dbfile/";
	tblName_ = {"region", "nation", "customer", "part", "partsupp", "supplier", "orders", "lineitem"};
	catalogPath_ = "catalog";
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
	for(auto &tn : tblName_) {
		if(tableInfo.find(tn) != tableInfo.end()) {
			continue;
		}
		TableInfo tblInfo;
		tblInfo.sch = std::move(Schema(catalogPath_.c_str(), tn.c_str()));
		tblInfo.dbf_path = dbfPrefix_ + tn + ".bin";

		tblInfo.loaded = true;
		// tblInfo.dbf.Create(tblInfo.dbf_path.c_str(), heap, 0);	
		// tblInfo.dbf.Close();
		tableInfo.emplace(tn, std::move(tblInfo));
	}
}

void Tables::Load(const char *tableName, std::string fileName) {
	if(tableInfo.find(tableName) == tableInfo.end()) {
		cerr << "ERROR: Attempt to load to non-exist table!\n";
		return;
	}
	TableInfo &tblInfo = tableInfo.at(tableName);
	char ch;
	if (tblInfo.loaded) {
		while (true) {
			cout << tableName << " has been loaded, do you want to load it again? (y or n)" << endl;
			cin >> ch;
			if('y' == ch) {				
				break;
			} else if ('n' == ch) {
				return;
			} 
		}	
		
	}
	cout << "Loading table " << tableName << " from " << fileName << endl;
	tblInfo.dbf.Open(tblInfo.dbf_path.c_str());
	string load_path(tblPrefix_ + std::string(tableName) + ".tbl");
	tblInfo.dbf.Load (tblInfo.sch, load_path.c_str());
	tblInfo.dbf.Close();
	tblInfo.loaded = true;
}

void Tables::LoadAll() {
	for(auto &tn : tblName_) {
		TableInfo &tblInfo = tableInfo.at(tn);
		if(tblInfo.loaded) {
			continue;
		}
		tblInfo.dbf.Open(tblInfo.dbf_path.c_str());
		string load_path(tblPrefix_ + tn + ".tbl");		
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
			return;
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

void Tables::AppendSchema(const char *tableName) {
	if(tableInfo.find(tableName) == tableInfo.end()) {
		cerr << "ERROR: Attempt to write schema of non-exist table!\n";
		return;
	}
	tableInfo.at(tableName).sch.Write(catalogPath_, tableName);
}

void Tables::RenewSchema() {
	ofstream outfile(catalogPath_,  ofstream::trunc);
	for (auto &rel : tableInfo) {
		rel.second.sch.Write(catalogPath_, rel.first);
	}
}

void Tables::Read(std::string filename) {
	ifstream infile(filename);
	if(!infile.is_open()) {
		cerr << "ERROR: Can't open table log file!\n";
		exit(1);
	}
	std::string tableName;
	while (true) {
		if(!(infile >> tableName)) {
			break;
		}
		TableInfo tblInfo;
		infile >> tblInfo.loaded;
		infile >> tblInfo.dbf_path;
		if(!ifstream(tblInfo.dbf_path).good()) {
			cerr << "Database file " << tableName << " has been deleted from disk, please create it again.\n";
			continue;
		}				
		tblInfo.sch = std::move(Schema(catalogPath_.c_str(), tableName.c_str()));	
		tableInfo.emplace(tableName, std::move(tblInfo));
	}

}

void Tables::Write(std::string filename) {
	ofstream outfile(filename);
	for(auto &tbl : tableInfo) {
		outfile << tbl.first << endl;
		outfile << tbl.second.loaded << endl;
		outfile << tbl.second.dbf_path << "\n" << endl;
	}
	
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
		// t.second.dbf.Close();
		// remove(t.first.c_str());
	}
	tableInfo.clear();
}
