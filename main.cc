
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
extern char* infileName;
extern struct AttList *attsList;
extern struct NameList *sortAtts;
extern int outputMode;
extern char* outfileName;

void SetOutput(int &outMode) ;

int main () {
	char ch;
	Statistics s;
	s.Read("STATS.txt");
	Tables table;
	int outMode = STDOUT_;
	cout << "\n*********************************************************************" << endl;
	cout << "                   Welcome to Rui's database                         " << endl;
	cout << "*********************************************************************" << endl;
	while(true) {
		cout << "\nPlease input instruction and press Ctrl + D to execute.\n" << endl;
		cout << ">>> ";
		if(yyparse() != 0) {
			cerr << "Please give valid input" << endl;
			continue;
			return 1;
		}
		cout << endl;
		switch (operationType) {
			case EXIT_:
				cout << "Thanks! Goodbye.\n" << endl;
				table.WriteSchema("newSchema.txt");
				table.Write("newTables.txt");
				s.Write("newStat.txt");
				exit(0);
				break;
			case CREATE_HEAP_:				
				table.Create(tableName, attsList);
				break;
			case CREATE_SORTED_:				
				table.Create(tableName, attsList, sortAtts);
				break;
			case CREATE_ALL_:
				table.CreateAll();
				break;
			case CREATE_ALL_NAME_:
				table.CreateAllName();
				break;	
			case INSERT_:				
				table.Load(tableName, infileName);
				break;
			case LOAD_ALL_:
				table.LoadAll();
				break;
			case DROP_:
				table.Drop(tableName);
				s.DropRel(tableName);
				break;
			case PRINT_TABLE_:
				table.Print();
				break;
			case INSERT_TABLE_:
				table.Read(infileName);
				break;
			case OUTPUT_:
				SetOutput(outMode);
				break;
			case PRINT_STATS_:
				s.Print();
				break;
			case INSERT_STATISTICS_:
				s.Read(infileName);
				break;
			case UPDATE_:
				table.UpdateStats(tableName, s);
				break;
			case QUERY_:				
				PlanTree planTree(s, table, outMode);
				if (DISCARD == planTree.GetPlanTree()) {
					continue;
				}		
				planTree.Execute();
				break;
		}		
	}	
}

void SetOutput(int &outMode) {
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
}
