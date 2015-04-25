
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "PlanTree.h"
#include "Statistics.h"
#include "Tables.h"
#include "time.h"
using namespace std;

extern "C" {
	struct YY_BUFFER_STATE *yy_scan_string(const char*);
	int yyparse(void);   // defined in y.tab.c
}

extern int operationType;
extern char* tableName;
extern char* dbfileName;
extern struct AttList *attsList;
extern struct NameList *sortAtts;
extern int outputMode;
extern char* outfileName;

int main () {
	char ch;
	Statistics s;
	s.Read("STATS.txt");
	std::unordered_map<std::string, TableInfo> tableInfo;
	Tables tables(tableInfo);
	tables.CreateAll();
	tables.LoadAll();	
	int outMode = OUTFILE_;
	cout << "\n*********************************************************************" << endl;
	cout << "                   Welcome to Rui's database                         " << endl;
	cout << "*********************************************************************" << endl;
	while(true) {		
		cout << "\nPlease input instruction and press Ctrl + D to execute.\n" << endl;
		cout << ">>> ";
		if(yyparse() != 0) {
			cerr << "Please give valid input" << endl;
			continue;
		}

		switch (operationType) {
			case CREATE_HEAP_:
				cout << "Creating heap file for table " << tableName << endl;
				tables.Create(tableName, attsList);
				break;
			case CREATE_SORTED_:
				cout << "Creating sorted file for table " << tableName << endl;
				tables.Create(tableName, attsList, sortAtts);
				break;
			case INSERT_:
				cout << "Loading table " << tableName << " from " << dbfileName << endl;
				tables.Load(tableName, dbfileName);
				break;
			case DROP_:
				char ch;				
				while (true) {
					cout << "Are you sure to delete table " << tableName << "? (y or n)" << endl;
					cin >> ch;
					if('y' == ch) {
						cout << "Deleting table " << tableName << endl;
						tables.Drop(tableName);
						break;
					} else if ('n' == ch) {
						break;
					} 
				}
				break;
			case PRINT_TABLE_:
				tables.Print();
				break;
			case OUTPUT_:
				cout << "Setting output mode:";
				switch (outputMode) {
					case STDOUT_:
						cout << "output to stdout" << endl;
						break;
					case OUTFILE_:
						cout << "output to file " << outfileName << endl;
						break;
					case NONE_:
						cout << "suppress output" << endl;
						break;
				}
				outMode = outputMode;
				break;
			case PRINT_STATS_:
				s.Print();
				break;
			case UPDATE_:
				cout << "Updating statistics for " << tableName << endl;
				cout << "not yet implemented!" << endl;
				break;
			case QUERY_:
				cout << "Executing query..." << endl;
				{
					PlanTree planTree(s, tableInfo);
					planTree.GetPlanTree(outMode);
					// double begin = clock();
					planTree.Execute();
					planTree.Wait();
					planTree.Clear();
					// double end = clock();
			 	// 	double cpu_time_used = (end - begin) / CLOCKS_PER_SEC;
			 	// 	cout << "Query spent " << cpu_time_used << endl;
			 	}
				break;			
			case EXIT_:
				cout << "Thanks! Goodbye." << endl;
				exit(0);
				break;
			default:
				break;
		}	
		cout << "\nSucceed.\n" << endl; 
	}
}


