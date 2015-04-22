#ifndef PLAN_TREE_H
#define PLAN_TREE_H

#include "Statistics.h"
#include "Schema.h"
#include "DBFile.h"
#include "RelOp.h"
#include "ParseInfo.h"
#include <cstring>
#include <string>

struct JoinRelInfo{
	int left;	// left relation index,
	int right;	// right relation index
	Predicate p;	//join predicate
};

struct CrossSelectInfo{
	std::vector<int> relNo;		// relation indices in the cross selection
	Predicate p;	// cross selcet predicate
};

struct TableInfo{
	DBFile dbf;	// corresponding DBFile
	Schema sch;	// corresponding schema
	TableInfo() = default;
	TableInfo(TableInfo &&rhs) {
		dbf = std::move(rhs.dbf);
		sch = std::move(rhs.sch);
	}
};

enum NodeType { Unary, Binary };
// plan node base class
class PlanNode {
public:
	NodeType type;	
	PlanNode *parent;	
	PlanNode *child;
	int inPipeID;	// input pipe ID
	int outPipeID;	// output pipe ID
	Schema *outSch;	// output schema
	double numTuples;	//number of tuples of the node 
	PlanNode() : type(Unary), parent(nullptr), child(nullptr), inPipeID(-1), outPipeID(-1), outSch(nullptr), numTuples(-1) { }
	virtual ~PlanNode() {
		if(child) {
			delete child;
			child = nullptr;
		}
	}
	virtual void Print() = 0;
};

class SelectFileNode : public PlanNode { 
private:
	DBFile *inFile;
	CNF *selOp;
	Record *literal;
public:
	SelectFileNode(DBFile *dbf, CNF *cnf, Record *rec) : inFile(dbf), selOp(cnf), literal(rec) { }
	SelectFileNode(const SelectFileNode &sf) = delete;
	SelectFileNode & operator =(const SelectFileNode &sf) = delete;
	void Print();
	~SelectFileNode() {
		delete selOp;		
		delete literal;		
	}
};

class SelectPipeNode : public PlanNode  {
private:
	CNF *selOp;
	Record *literal;
public:
	SelectPipeNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec) { }
	SelectPipeNode(const SelectPipeNode &sf) = delete;
	SelectPipeNode & operator =(const SelectPipeNode &sf) = delete;
	void Print();
	~SelectPipeNode() {
		delete selOp;
		delete literal;		
	}
};

class ProjectNode : public PlanNode { 
private:
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
public:
	ProjectNode(int *atts, int numIn, int numOut) : keepMe(atts), numAttsInput(numIn), numAttsOutput(numOut) { }
	ProjectNode(const ProjectNode &pn) = delete;
	ProjectNode & operator =(const ProjectNode &pn) = delete;
	void Print();
	~ProjectNode() {
		delete[] keepMe;
		delete outSch;
	}
};

class JoinNode : public PlanNode { 
private:
	CNF *selOp;
	Record *literal;
public:
	PlanNode *rchild;
	int rightInPipeID;
	JoinNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec), rchild(nullptr), rightInPipeID(-1) { }
	JoinNode(const JoinNode &jn) = delete;
	JoinNode & operator = (const JoinNode &jn) = delete;
	void Print();
	~JoinNode() {
		delete selOp;
		delete literal;	
		delete outSch;
		if(rchild) {
			delete rchild;
			rchild = nullptr;
		}
	}
};

class DuplicateRemovalNode : public PlanNode {
private:
	Schema *mySchema;
public:
	DuplicateRemovalNode(Schema * sch) : mySchema(sch) { }
	void Print();
	~DuplicateRemovalNode() {
		// nothing
	}
};

class SumNode : public PlanNode {
private:
	Function *computeMe;
public:	
	SumNode(Function *func) : computeMe(func) { }
	SumNode(const SumNode &sn) = delete;
	SumNode & operator = (const SumNode &sn) = delete;
	void Print();
	~SumNode() {
		delete computeMe;
		delete outSch;
	}
};

class GroupByNode : public PlanNode {
private:
	OrderMaker *groupAtts;
	Function *computeMe;
public:
	GroupByNode(OrderMaker *om, Function *func) : groupAtts(om), computeMe(func) { }
	GroupByNode(const GroupByNode *gn) = delete;
	GroupByNode & operator = (const GroupByNode *gn) = delete;
	void Print();
	~GroupByNode() {
		delete groupAtts;
		delete computeMe;
		delete outSch;
	}
};

class WriteOutNode : public PlanNode {
private:
	FILE *outFile;
	Schema *mySchema;
	std::string filename;
public:
	WriteOutNode(FILE *f, Schema *sch, std::string name) : outFile(f), mySchema(sch), filename(name) { }
	WriteOutNode(const WriteOutNode &wn) = delete;
	WriteOutNode & operator =(const WriteOutNode &wn) = delete;
	void Print();
	~WriteOutNode() {
		// nothing
	}
};

class PlanTree {
private:
	PlanNode *root;
	Statistics &s;
	int numOfRels;	// store number of relations in the tree	
	std::vector<char*> relNames;	// store all the alias relation name	
	std::unordered_map<string, string> tableList;	// hash table for alias and corresponding origin realtion name	
	std::unordered_map<std::string, TableInfo> tableInfo;	// hash table for relation name and relation infomation
	std::unordered_map<string, Predicate> selectList;	// hash table for relation name and selection predicate on this relation
	std::vector<JoinRelInfo> joinList;	//set of join predicate
	std::vector<CrossSelectInfo> crossSelectList;	// set of cross select predicate(that one selection predicate select on more than one attribute) 
	std::vector<PlanNode*> selectFileList;
	std::vector<PlanNode*> buildedNodes;	//store builded node expect select file node, which stores in selectFileList

	// separate select, join and cross select predicate
	void SeparatePredicate(struct AndList *parseTree);
	// check if any cross select predicate can be apply
	int CheckCrossSelect(std::vector<CrossSelectInfo> &csl, std::vector<int> &joinedTable);	
	// set up necessary table infomation for each relation in table name
	void CreateTable(char* tableName);
	// get the type of result of function
	Type GetSumType(Function *func);

	void GrowSelectFileNode();
	void GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels);
	// Grow first join node, which join two select file node
	void GrowRowJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList);
	// Grow the cooked join node, which join one select file node and a (join node | select pipe node)
	void GrowCookedJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels);
	void GrowProjectNode (struct NameList *attsToSelect);
	void GrowDuplicateRemovalNode ();
	void GrowSumNode (struct FuncOperator *finalFunction);
	void GrowGroupByNode (struct FuncOperator *finalFunction, struct NameList *groupingAtts);
	void GrowWriteOutNode(const char* filename);

	void BuildUnaryNode(PlanNode *child, PlanNode *parent);
	void BuildBinaryNode (PlanNode *lchild, PlanNode *rchild, JoinNode *parent, int outID);
	void PrintNode(PlanNode *root);

public:
	PlanTree(Statistics &stat);
	~PlanTree() {
		delete root;
	}
	void BuildTableList(struct TableList *tables);
	void GetPlanTree(struct AndList *pAnd);
	void PrintTree();	
};


#endif