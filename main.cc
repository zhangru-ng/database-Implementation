
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

void SetOutput(int &outMode) ;

int main () {
	char ch;
	Statistics s;
	s.Read("STATS.txt");
	Tables table;
	table.CreateAll();
	table.LoadAll();	
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
			case EXIT_:
				cout << "Thanks! Goodbye." << endl;
				exit(0);
				break;
			case CREATE_HEAP_:				
				table.Create(tableName, attsList);
				break;
			case CREATE_SORTED_:				
				table.Create(tableName, attsList, sortAtts);
				break;
			case INSERT_:				
				table.Load(tableName, dbfileName);
				break;
			case DROP_:
				table.Drop(tableName);
				break;
			case PRINT_TABLE_:
				table.Print();
				break;
			case OUTPUT_:
				SetOutput(outMode);
				break;
			case PRINT_STATS_:
				s.Print();
				break;
			case UPDATE_:
				cout << "Updating statistics for " << tableName << endl;
				cout << "not yet implemented!" << endl;
				break;
			case QUERY_:				
				PlanTree planTree(s, table, outMode);
				if (DISCARD == planTree.GetPlanTree()) {
					continue;
				}
				planTree.Execute();
				break;
		}	
		cout << "\nSucceed.\n" << endl; 
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
