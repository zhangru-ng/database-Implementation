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

	int Run(const Record *left, const Record *literal, const Comparison *c)const;
	int Run(const Record *left, const Record *right, const Record *literal, const Comparison *c)const;

public:

        // this version of Compare is for sorting.  The OrderMaker struct
        // encapsulates the specification for a sort order.  For example,
        // say you are joining two tables on R.att1 = S.att2, and so
        // you want to sort R using att1.  The OrderMaker struct specifies
        // this sort ordering on att1.  Compare returns a negative number,
        // a 0, or a positive number if left is less than, equal to, or
        // greater than right.   This particular version of Compare is used
        // when both of the records come from the SAME RELATION
	int Compare(const Record * left, const Record * right, const OrderMaker * orderUs) const ; 

	/********************Added by Rui: 2015.2.28**********************************/
	//attributes permutation in literal record is compress(add one at a time from 0) 
	//and not the same as Ordermaker, the a subscripte converting arry litOrder 
	//match the subscript of literal to OrderMaker.whichAtts[]
	int Compare(const Record *left, const Record *literal, const OrderMaker *orderUs, const int *litOrder) const;

	// similar to the last function, except that this one works in the
        // case where the two records come from different input relations
	// it is used to do sorts for a sort-merge join
	int Compare(const Record *left, const OrderMaker *order_left, const Record *right, const OrderMaker *order_right) const;

	// this applies the given CNF to the three records and either 
	// accepts the records or rejects them.
        // It is is for binary operations such as join.  Returns
        // a 0 if the given CNF evaluates to false over the record pair
	int Compare(const Record *left, const Record *right, const Record *literal, const CNF *myComparison)const;

	// like the last one, but for unary operations
	int Compare(const Record *left, const Record *literal, const CNF *myComparison)const;


};

#endif
