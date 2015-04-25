
#ifndef SCHEMA_H
#define SCHEMA_H

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

struct att_pair {
	char *name;
	Type type;
};

struct Attribute {
	std::string name;
	Type myType;
};

class OrderMaker;
class Schema {

	// gives the attributes in the schema
	int numAtts;
	Attribute *myAtts;

	// gives the physical location of the binary file storing the relation
	std::string fileName;

	friend class Record;

public:

	// gets the set of attributes, but be careful with this, since it leads
	// to aliasing!!!
	Attribute *GetAtts ();

	// returns the number of attributes
	int GetNumAtts ();

	// this finds the position of the specified attribute in the schema
	// returns a -1 if the attribute is not present in the schema
	int Find (char *attName);

	// this finds the type of the given attribute
	Type FindType (char *attName);

	// finds the type of the given index
	Type GetType (int index);

	// this reads the specification for the schema in from a file
	Schema (const char *fName, const char *relName);

	// this composes a schema instance in-memory
	Schema (const char *fName, int num_atts, Attribute *atts);

	// compose a schema using input attribute list in CREATE TABLE
	Schema (struct AttList *attsList);

	// this constructs a sort order structure that can be used to
	// place a lexicographic ordering on the records using this type of schema
	int GetSortOrder (OrderMaker &order);

	// Merge two schema into one
	Schema (const Schema &left, const Schema &right);

	// print the schema
	void Print() const;

	Schema();

	// move constructor
	Schema(Schema &&rhs);

	// move assignment
	Schema& operator = (Schema &&rhs);

	~Schema ();

};

#endif
