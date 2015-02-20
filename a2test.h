#ifndef TEST_H
#define TEST_H
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"

using namespace std;

// make sure that the information below is correct

char *catalog_path = "catalog"; 
char *tpch_dir ="/cise/homes/rui/Desktop/testfile/100M_table/"; // dir where dbgen tpch files (extension *.tbl) can be found
char *dbfile_dir = "dbfile/"; 


extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	bool print;
	bool write;
}testutil;

class relation {

private:
	char *rname;
	char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (char *_name, Schema *_schema, char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) {
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	char* name () { return rname; }
	char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info () {
		cout << " relation info\n";
		cout << "\t name: " << name () << endl;
		cout << "\t path: " << path () << endl;
	}

	void get_cnf (CNF &cnf_pred, Record &literal) {
		cout << " Enter CNF predicate (when done press ctrl-D):\n\t";
  		if (yyparse() != 0) {
			cout << "Can't parse your CNF.\n";
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
	}
	void get_sort_order (OrderMaker &sortorder) {
		cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
  		if (yyparse() != 0) {
			cout << "Can't parse your sort CNF.\n";
			exit (1);
		}
		cout << " \n";
		Record literal;
		CNF sort_pred;
		sort_pred.GrowFromParseTree (final, schema (), literal); // constructs CNF predicate
		OrderMaker dummy;
		if( !sort_pred.GetSortOrders (sortorder, dummy) ){
			cerr << "it is impossible to determine an acceptable ordering for the given comparison";
			exit(1);
		}
	}
};


relation *rel;


char *supplier = "supplier"; 
char *partsupp = "partsupp"; 
char *part = "part"; 
char *nation = "nation"; 
char *customer = "customer"; 
char *orders = "orders"; 
char *region = "region"; 
char *lineitem = "lineitem"; 

relation *s, *p, *ps, *n, *li, *r, *o, *c;
//added by rui, prevent memory leak
Schema *ssc, *psc, *pssc, *nsc, *lisc, *rsc, *osc, *csc;

void setup () {
	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
	cout << " catalog location: \t" << catalog_path << endl;
	cout << " tpch files dir: \t" << tpch_dir << endl;
	cout << " heap files dir: \t" << dbfile_dir << endl;
	cout << " \n\n";

	//added by rui, prevent memory leak
	ssc = new Schema (catalog_path, supplier);
	pssc = new Schema (catalog_path, partsupp);
	psc = new Schema (catalog_path, part); 
	nsc = new Schema (catalog_path, nation);
	lisc = new Schema (catalog_path, lineitem);
	rsc= new Schema (catalog_path, region);
	osc = new Schema (catalog_path, orders);
	csc = new Schema (catalog_path, customer);

	s = new relation (supplier, ssc, dbfile_dir);
	ps = new relation (partsupp, pssc, dbfile_dir);
	p = new relation (part, psc, dbfile_dir);
	n = new relation (nation, nsc, dbfile_dir);
	li = new relation (lineitem, lisc, dbfile_dir);
	r = new relation (region, rsc, dbfile_dir);
	o = new relation (orders, osc, dbfile_dir);
	c = new relation (customer, csc, dbfile_dir);
}

void cleanup () {
	delete s;
	delete p;
	delete ps; 
	delete n;
	delete li;
	delete r;
	delete o;
	delete c;

	//added by rui, prevent memory leak
	delete ssc;
	delete psc;
	delete pssc; 
	delete nsc;
	delete lisc;
	delete rsc;
	delete osc;
	delete csc;
}

#endif
