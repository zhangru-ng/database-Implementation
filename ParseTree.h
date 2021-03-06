
#ifndef ParseTree
#define ParseTree
// these are the types of operands that can appear in a CNF expression
#define INT 1
#define DOUBLE 2
#define STRING 3
#define NAME 4
#define LESS_THAN 5
#define GREATER_THAN 6
#define EQUALS 7
#define CREATE_HEAP_ 8
#define CREATE_SORTED_ 9
#define INSERT_ 10
#define DROP_ 11
#define OUTPUT_ 12
#define UPDATE_ 13
#define QUERY_ 14
#define STDOUT_ 15
#define OUTFILE_ 16
#define NONE_ 17
#define PRINT_TABLE_ 18
#define PRINT_STATS_ 19
#define EXIT_ 20
#define CREATE_ALL_ 21
#define LOAD_ALL_ 22
#define CREATE_ALL_NAME_ 23
#define INSERT_TABLE_ 24
#define INSERT_STATISTICS_ 25

// used in computational (funcional) expressions
struct FuncOperand {

	// this tells us the type of the operand: FLOAT, INT, STRING...
	int code;

	// this is the actual operand
	char* value;
};

struct FuncOperator {

	// this tells us which operator to use: '+', '-', ...
	int code;

	// these are the operators on the left and on the right
	struct FuncOperator *leftOperator;
	struct FuncOperand *leftOperand;
	struct FuncOperator *right;	

};

struct TableList {

	// this is the original table name
	char* tableName;

	// this is the value it is aliased to
	char* aliasAs;

	// and this the next alias
	struct TableList *next;
};

struct NameList {

	// this is the name
	char* name;

	// and this is the next name in the list
	struct NameList *next;
};

// used in boolean expressions... there's no reason to have both this
// and FuncOperand, but both are here for legacy reasons!!
struct Operand {

        // this tells us the type of the operand: FLOAT, INT, STRING...
        int code;

        // this is the actual operand
        char* value;
};

struct ComparisonOp {

        // this corresponds to one of the codes describing what type
        // of literal value we have in this nodes: LESS_THAN, EQUALS...
        int code;

        // these are the operands on the left and on the right
        struct Operand *left;
        struct Operand *right;
};

struct OrList {

        // this is the comparison to the left of the OR
        struct ComparisonOp *left;

        // this is the OrList to the right of the OR; again,
        // this might be NULL if the right is a simple comparison
        struct OrList *rightOr;
};

struct AndList {

        // this is the disjunction to the left of the AND
        struct OrList *left;

        // this is the AndList to the right of the AND
        // note that this can be NULL if the right is a disjunction
        struct AndList *rightAnd;

};

struct AttList {

	// this is the input attribute name
	char* name;

	// this is the input attribute type
	int code;

	struct AttList *next;
	
};

#endif
