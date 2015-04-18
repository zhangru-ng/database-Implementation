#ifndef PLAN_TREE_H
#define PLAN_TREE_H

#include "Statistics.h"
#include "Schema.h"
#include "DBFile.h"
#include "RelOp.h"
#include <cstring>

typedef struct AndList* Predicate;

typedef struct {
	int left;
	int right;
	Predicate p;
}JoinRelInfo;

typedef struct {
	std::vector<int> relNo;
	Predicate p;
}CrossSelectInfo;

typedef struct {
	DBFile *dbf;
	Schema *sch;
	Predicate initPred;
}TableInfo;

class PlanNode;

class PlanTree {
private:
	PlanNode *root;
	Statistics &s;
	int numToJoin;

	std::vector<char*> relNames;
	std::unordered_map<char *, char *> tableList;
	std::unordered_map<std::string, TableInfo> tableInfo;
	std::unordered_map<char*, Predicate> selectList;
	std::vector<JoinRelInfo> joinList;
	std::vector<CrossSelectInfo> crossSelectList;


	void SeparatePredicate(struct AndList *parseTree);
	int CheckCrossSelect(std::vector<CrossSelectInfo> &csl, std::vector<int> &joinedTable);	
	void InitPredicate(char *attName, Predicate &initPred);
	void CreateTable(char* tableName);
	void RemovePrefix(struct Operand *oper);
public:
	PlanTree(Statistics &stat);
	void BuildTableList(struct TableList *tables);
	void GetJoinOrder(struct AndList *pAnd);
	
};

enum NodeType { Unary, Binary };

class PlanNode {
public:
	PlanNode *parent;
	NodeType type;	
	int outPipeID;
	Schema *outSch;
	PlanNode() : parent(nullptr), type(Unary), outPipeID(-1) { }
	// virtual ~PlanNode();
};

class UnaryNode : public PlanNode {
public:
	PlanNode *child;
	int inPipeID;

	UnaryNode() : child(nullptr), inPipeID(-1) { }
	// virtual ~UnaryNode();
};

class BinaryNode : public PlanNode {
public:
	PlanNode *lchild;
	PlanNode *rchild;
	int leftInPipeID;
	int rightInPipeID;

	BinaryNode() : lchild(nullptr), rchild(nullptr), leftInPipeID(-1), rightInPipeID(-1) { }
	// virtual ~BinaryNode();
};


class SelectFileNode : public UnaryNode { 
private:
	DBFile *inFile;
	CNF *selOp;
	Record *literal;
public:
	SelectFileNode(DBFile *dbf, CNF *cnf, Record *rec) : inFile(dbf), selOp(cnf), literal(rec) { }
	SelectFileNode(const SelectFileNode &sf) = delete;
	SelectFileNode & operator =(const SelectFileNode &sf) = delete;
	~SelectFileNode() {
		delete selOp;
		delete literal;
	}
};

class SelectPipeNode : public UnaryNode  {
private:
	CNF *selOp;
	Record *literal;
public:
	SelectPipeNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec) { }
	SelectPipeNode(const SelectPipeNode &sf) = delete;
	SelectPipeNode & operator =(const SelectPipeNode &sf) = delete;
	~SelectPipeNode() {
		delete selOp;
		delete literal;
	}
};

class ProjectNode : public UnaryNode { 
private:
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
public:
	ProjectNode(int *atts, int numIn, int numOut) : keepMe(atts), numAttsInput(numIn), numAttsOutput(numOut) { }
	ProjectNode(const ProjectNode &pn) = delete;
	ProjectNode & operator =(const ProjectNode &pn) = delete;
};

class JoinNode : public BinaryNode { 
private:
	CNF *selOp;
	Record *literal;
public:
	JoinNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec) { }
	JoinNode(const JoinNode &jn) = delete;
	JoinNode & operator = (const JoinNode &jn) = delete;
	~JoinNode() {
		delete selOp;
		delete literal;
	}
};

class DuplicateRemovalNode : public UnaryNode {
private:
	Schema *mySchema;
public:
	DuplicateRemovalNode(Schema * sch) : mySchema(sch) { }
};

class SumNode : public UnaryNode {
private:
	Function *computeMe;
public:	
	SumNode(Function *func) : computeMe(func) { }
	SumNode(const SumNode &sn) = delete;
	SumNode & operator = (const SumNode &sn) = delete;
};

class GroupByNode : public UnaryNode {
private:
	OrderMaker *groupAtts;
	Function *computeMe;
public:
	GroupByNode(OrderMaker *om, Function *func) : groupAtts(om), computeMe(func) { }
	GroupByNode(const GroupByNode *gn) = delete;
	GroupByNode & operator = (const GroupByNode *gn) = delete;
};

class WriteOutNode : public UnaryNode {
private:
	FILE *outFile;
	Schema *mySchema;
public:
	WriteOutNode(FILE *f, Schema *sch) : outFile(f), mySchema(sch) { }
	WriteOutNode(const WriteOutNode &wn) = delete;
	WriteOutNode & operator =(const WriteOutNode &wn) = delete;
};


#endif