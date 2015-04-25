#include "Schema.h"
#include <string.h>
#include <stdio.h>
#include <iostream>

int Schema :: Find (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!myAtts[i].name.compare(attName)) {
			return i;
		}
	}

	// if we made it here, the attribute was not found
	return -1;
}

Type Schema :: FindType (char *attName) {

	for (int i = 0; i < numAtts; i++) {
		if (!myAtts[i].name.compare(attName)) {
			return myAtts[i].myType;
		}
	}

	// if we made it here, the attribute was not found
	return Int;
}

Type Schema :: GetType (int index) {
	if (index >= numAtts) {
		cerr << "ERROR: Index out of bound!\n";
		exit(1);
	}
	return myAtts[index].myType;
}

int Schema :: GetNumAtts () {
	return numAtts;
}

Attribute *Schema :: GetAtts () {
	return myAtts;
}

void Schema :: Print () const {
	for (int i = 0; i < numAtts; i++ ) {
		cout << "No." << i << "  " << myAtts[i].name << "  ";
		if (myAtts[i].myType == Int) {
			cout << "Int" << endl;
		} else if (myAtts[i].myType == Double) {
			cout << "Double" << endl;
		} else if(myAtts[i].myType == String) {
			cout << "String" << endl;
		} 		
	}
}

Schema :: Schema() {
	numAtts = 0;
	myAtts = NULL;
}

Schema :: Schema(Schema &&rhs) {
	numAtts = rhs.numAtts;
	myAtts = rhs.myAtts;
	rhs.myAtts = nullptr;
	fileName = std::move(rhs.fileName);
}

Schema& Schema :: operator = (Schema &&rhs) {
	numAtts = rhs.numAtts;
	myAtts = rhs.myAtts;
	rhs.myAtts = nullptr;
	fileName = std::move(rhs.fileName);
	return *this;
}

Schema :: Schema (struct AttList *attsList) {
	struct AttList *p = attsList;
	numAtts = 0;
	while (p) {
		++numAtts;
		p = p->next;
	}
	myAtts = new Attribute[numAtts];
	p = attsList;
	for (int i = 0; i < numAtts; i++) {
		myAtts[i].name = strdup(p->name);
		if (p->code == INT) {
			myAtts[i].myType = Int;
		} else if (p->code == DOUBLE) {
			myAtts[i].myType = Double;
		} else if (p->code == STRING) {
			myAtts[i].myType = String;
		} 
		p = p->next;
	}
}

Schema :: Schema (const Schema &left, const Schema &right) {
	numAtts = left.numAtts + right.numAtts;
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++) {
		Type attType;
		if(i < left.numAtts) {
			attType = left.myAtts[i].myType;
			myAtts[i].name = left.myAtts[i].name;
		} else {
			attType = right.myAtts[i - left.numAtts].myType;
			myAtts[i].name = right.myAtts[i - left.numAtts].name;
		}
		if (attType == Int) {
			myAtts[i].myType = Int;
		}
		else if (attType == Double) {
			myAtts[i].myType = Double;
		}
		else if (attType == String) {
			myAtts[i].myType = String;
		} 
		else {
			cout << "Bad attribute type for " << attType << "\n";
			delete [] myAtts;
			exit (1);
		}
	}
}

Schema :: Schema (const char *fpath, int num_atts, Attribute *atts) {
	fileName = fpath;
	numAtts = num_atts;
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {
		if (atts[i].myType == Int) {
			myAtts[i].myType = Int;
		}
		else if (atts[i].myType == Double) {
			myAtts[i].myType = Double;
		}
		else if (atts[i].myType == String) {
			myAtts[i].myType = String;
		} 
		else {
			cout << "Bad attribute type for " << atts[i].myType << "\n";
			delete [] myAtts;
			exit (1);
		}
		myAtts[i].name = atts[i].name;
	}
}

Schema :: Schema (const char *fName, const char *relName) {

	FILE *foo = fopen (fName, "r");
	
	// this is enough space to hold any tokens
	char space[200];

	fscanf (foo, "%s", space);
	int totscans = 1;

	// see if the file starts with the correct keyword
	if (strcmp (space, "BEGIN")) {
		cout << "Unfortunately, this does not seem to be a schema file.\n";
		exit (1);
	}	
		
	while (1) {

		// check to see if this is the one we want
		fscanf (foo, "%s", space);
		totscans++;
		if (strcmp (space, relName)) {

			// it is not, so suck up everything to past the BEGIN
			while (1) {

				// suck up another token
				if (fscanf (foo, "%s", space) == EOF) {
					cerr << "Could not find the schema for the specified relation.\n";
					exit (1);
				}

				totscans++;
				if (!strcmp (space, "BEGIN")) {
					break;
				}
			}

		// otherwise, got the correct file!!
		} else {
			break;
		}
	}

	// suck in the file name
	fscanf (foo, "%s", space);
	totscans++;
	fileName = space;

	// count the number of attributes specified
	numAtts = 0;
	while (1) {
		fscanf (foo, "%s", space);
		if (!strcmp (space, "END")) {
			break;		
		} else {
			fscanf (foo, "%s", space);
			numAtts++;
		}
	}

	// now actually load up the schema
	fclose (foo);
	foo = fopen (fName, "r");

	// go past any un-needed info
	for (int i = 0; i < totscans; i++) {
		fscanf (foo, "%s", space);
	}

	// and load up the schema
	myAtts = new Attribute[numAtts];
	for (int i = 0; i < numAtts; i++ ) {

		// read in the attribute name
		fscanf (foo, "%s", space);	
		myAtts[i].name = space;

		// read in the attribute type
		fscanf (foo, "%s", space);
		if (!strcmp (space, "Int")) {
			myAtts[i].myType = Int;
		} else if (!strcmp (space, "Double")) {
			myAtts[i].myType = Double;
		} else if (!strcmp (space, "String")) {
			myAtts[i].myType = String;
		} else {
			cout << "Bad attribute type for " << myAtts[i].name << "\n";
			exit (1);
		}
	}
	
	fclose (foo);
}

Schema :: ~Schema () {
	delete [] myAtts;	
	myAtts = NULL;
}

