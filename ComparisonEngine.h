#ifndef _ENGINE
#define _ENGINE


#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class Record;
class Comparison;
class OrderMaker;
class CNF;

class ComparisonEngine {

private:

	int Run(Record *left, Record *literal, Comparison *c);
	int Run(Record *left, Record *right, Record *literal, Comparison *c);

public:

        // this version of Compare is for sorting.  The OrderMaker struct
        // encapsulates the specification for a sort order.  For example,
        // say you are joining two tables on R.att1 = S.att2, and so
        // you want to sort R using att1.  The OrderMaker struct specifies
        // this sort ordering on att1.  Compare returns a negative number,
        // a 0, or a positive number if left is less than, equal to, or
        // greater than right.   This particular version of Compare is used
        // when both of the records come from the SAME RELATION
	int Compare(Record *left, Record *right, OrderMaker *orderUs);

	/********************Added by Rui: 2015.2.28**********************************/
	//attributes permutation in literal record is compress(add one at a time from 0) 
	//and not the same as Ordermaker, the a subscripte converting arry litOrder 
	//match the subscript of literal to OrderMaker.whichAtts[]
	int Compare(Record *left, Record *literal, OrderMaker *orderUs, int *litOrder);

	// similar to the last function, except that this one works in the
        // case where the two records come from different input relations
	// it is used to do sorts for a sort-merge join
	int Compare(Record *left, OrderMaker *order_left, Record *right, OrderMaker *order_right);

	// this applies the given CNF to the three records and either 
	// accepts the records or rejects them.
        // It is is for binary operations such as join.  Returns
        // a 0 if the given CNF evaluates to false over the record pair
	int Compare(Record *left, Record *right, Record *literal, CNF *myComparison);

	// like the last one, but for unary operations
	int Compare(Record *left, Record *literal, CNF *myComparison);


};

#endif
