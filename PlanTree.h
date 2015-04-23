#ifndef PLAN_TREE_H
#define PLAN_TREE_H

#include "Statistics.h"
#include "Schema.h"
#include "DBFile.h"
#include "RelOp.h"
#include "Tables.h"
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

class PlanTree;

enum NodeType { Unary, Binary };

class PlanNodeVisitor;

// plan node base class
class PlanNode {
public:
	static std::vector<Pipe *> pipePool;
	NodeType type;	
	PlanNode *parent;	
	PlanNode *child;
	int inPipeID;	// input pipe ID
	int outPipeID;	// output pipe ID
	Schema *outSch;	// output schema
	double numTuples;	//number of tuples of the node 
	PlanNode();
	virtual ~PlanNode();
	virtual void Visit(PlanNodeVisitor &v) = 0;
	static void ClearPipePool();
};

class SelectFileNode : public PlanNode { 
public:
	DBFile *inFile;
	CNF *selOp;
	Record *literal;
	SelectFile sf;
	SelectFileNode(DBFile *dbf, CNF *cnf, Record *rec);
	SelectFileNode(const SelectFileNode &sf) = delete;
	SelectFileNode & operator =(const SelectFileNode &sf) = delete;
	void Visit(PlanNodeVisitor &v);
	~SelectFileNode();
};

class SelectPipeNode : public PlanNode  {
public:
	CNF *selOp;
	Record *literal;
	SelectPipe sp;
	SelectPipeNode(CNF *cnf, Record *rec);
	SelectPipeNode(const SelectPipeNode &sf) = delete;
	SelectPipeNode & operator =(const SelectPipeNode &sf) = delete;
	void Visit(PlanNodeVisitor &v);
	~SelectPipeNode();
};

class ProjectNode : public PlanNode { 
public:
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	Project pj;
	ProjectNode(int *atts, int numIn, int numOut);
	ProjectNode(const ProjectNode &pn) = delete;
	ProjectNode & operator =(const ProjectNode &pn) = delete;
	void Visit(PlanNodeVisitor &v);
	~ProjectNode();
};

class JoinNode : public PlanNode { 
public:
	CNF *selOp;
	Record *literal;
	Join jn;
	PlanNode *rchild;
	int rightInPipeID;
	JoinNode(CNF *cnf, Record *rec);
	JoinNode(const JoinNode &jn) = delete;
	JoinNode & operator = (const JoinNode &jn) = delete;
	void Visit(PlanNodeVisitor &v);
	~JoinNode();
};

class DuplicateRemovalNode : public PlanNode {
public:
	Schema *mySchema;
	DuplicateRemoval dr;
	DuplicateRemovalNode(Schema * sch);
	DuplicateRemovalNode(const DuplicateRemovalNode &dr) = delete;
	DuplicateRemovalNode& operator = (const DuplicateRemovalNode &dr) = delete ;
	void Visit(PlanNodeVisitor &v);
	~DuplicateRemovalNode();
};

class SumNode : public PlanNode {
public:
	Function *computeMe;
	Sum sm;
	SumNode(Function *func);
	SumNode(const SumNode &sn) = delete;
	SumNode & operator = (const SumNode &sn) = delete;
	void Visit(PlanNodeVisitor &v);
	~SumNode();
};

class GroupByNode : public PlanNode {
public:
	OrderMaker *groupAtts;
	Function *computeMe;
	GroupBy gb;
	GroupByNode(OrderMaker *om, Function *func);
	GroupByNode(const GroupByNode *gn) = delete;
	GroupByNode & operator = (const GroupByNode *gn) = delete;
	void Visit(PlanNodeVisitor &v);
	~GroupByNode();
};

class WriteOutNode : public PlanNode {
public:
	FILE *outFile;
	Schema *mySchema;
	std::string filename;
	int outputMode;
	WriteOut wo;
	WriteOutNode(Schema *sch, int mode);
	WriteOutNode(FILE *f, Schema *sch, std::string name, int mode);
	WriteOutNode(const WriteOutNode &wn) = delete;
	WriteOutNode & operator =(const WriteOutNode &wn) = delete;
	void Visit(PlanNodeVisitor &v);
	~WriteOutNode();
};

class PlanNodeVisitor {
public:
	virtual void VisitSelectFileNode(SelectFileNode *node) = 0;
	virtual void VisitSelectPipeNode(SelectPipeNode *node) = 0;
	virtual void VisitJoinNode(JoinNode *node) = 0;
	virtual void VisitProjectNode(ProjectNode *node) = 0;
	virtual void VisitDuplicateRemovalNode(DuplicateRemovalNode *node) = 0;
	virtual void VisitSumNode(SumNode *node) = 0;
	virtual void VisitGroupByNode(GroupByNode *node) = 0;
	virtual void VisitWriteOutNode(WriteOutNode *node) = 0;
};

class PrintVisitor : public PlanNodeVisitor {
public:
	void VisitSelectFileNode(SelectFileNode *node);
	void VisitSelectPipeNode(SelectPipeNode *node);
	void VisitJoinNode(JoinNode *node);
	void VisitProjectNode(ProjectNode *node);
	void VisitDuplicateRemovalNode (DuplicateRemovalNode *node);
	void VisitSumNode(SumNode *node);
	void VisitGroupByNode(GroupByNode *node);
	void VisitWriteOutNode(WriteOutNode *node);
};

class RunVisitor : public PlanNodeVisitor {
public:
	void VisitSelectFileNode(SelectFileNode *node);
	void VisitSelectPipeNode(SelectPipeNode *node);
	void VisitJoinNode(JoinNode *node);
	void VisitProjectNode(ProjectNode *node);
	void VisitDuplicateRemovalNode(DuplicateRemovalNode *node);
	void VisitSumNode(SumNode *node);
	void VisitGroupByNode(GroupByNode *node);
	void VisitWriteOutNode(WriteOutNode *node);
};


class PlanTree {
private:
	static const int pipesz = 100;
	PlanNode *root;
	int numOfRels;	// store number of relations in the tree	
	Statistics &stat;
	std::unordered_map<std::string, TableInfo> &tableInfo;	// hash table for relation name and relation infomation	
	std::vector<char*> relNames;	// store all the alias relation name	
	std::unordered_map<string, string> tableList;	// hash table for alias and corresponding origin realtion name		
	std::unordered_map<string, Predicate> selectList;	// hash table for relation name and selection predicate on this relation
	std::vector<JoinRelInfo> joinList;	//set of join predicate
	std::vector<CrossSelectInfo> crossSelectList;	// set of cross select predicate(that one selection predicate select on more than one attribute) 
	std::vector<PlanNode*> selectFileList;
	std::vector<PlanNode*> buildedNodes;	//store builded node expect select file node, which stores in selectFileList

	// separate select, join and cross select predicate
	void SeparatePredicate(struct AndList *parseTree);
	// check if any cross select predicate can be apply
	int CheckCrossSelect(std::vector<CrossSelectInfo> &csl, std::vector<int> &joinedTable);	
	// check if the tables in predicates exist in database
	void CheckRels(const char* relName);
	// get the type of result of function
	Type GetSumType(Function *func);

	void GrowSelectFileNode();
	void GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels);
	// Grow first join node, which join two select file node
	void GrowRowJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList);
	// Grow the cooked join node, which join one select file node and a (join node | select pipe node)
	void GrowCookedJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels);
	void GrowProjectNode (struct NameList *attsToSelect);
	void GrowDuplicateRemovalNode();
	void GrowSumNode(struct FuncOperator *finalFunction);
	void GrowGroupByNode(struct FuncOperator *finalFunction, struct NameList *groupingAtts);
	void GrowWriteOutNode(const char* filename, int outputMode);

	void BuildUnaryNode(PlanNode *child, PlanNode *parent);
	void BuildBinaryNode (PlanNode *lchild, PlanNode *rchild, JoinNode *parent, int outID);
	void VisitNode(PlanNode *root, PlanNodeVisitor &v);

public:
	PlanTree(Statistics &stat, std::unordered_map<std::string, TableInfo> &tableInfo);
	~PlanTree();
	void BuildTableList(struct TableList *tables);
	void GetPlanTree(struct AndList *pAnd);
	void VisitTree(PlanNodeVisitor &v);	
};


#endif