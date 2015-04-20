#include "PlanTree.h"

string tbl_prefix = "/cise/homes/rui/Desktop/testfiles/10M_table/";
string dbf_prefix = "dbfile/";
string tbl_name[8] = {"region", "nation", "customer", "part", "partsupp", "supplier", "order", "lineitem"};
enum Table { REGION = 0, NATION, CUSTOMER, PART, PARTSUPP, SUPPLIER, ORDER, LINEITEM };
string catalog_path = "catalog";
extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

PlanTree::PlanTree(Statistics &stat) : root(nullptr), s(stat), numToJoin(0) { }

void PlanTree::BuildTableList(struct TableList *tables) {
	struct TableList *p = tables;
	while (p) {
		if (p->tableName) {
			s.CheckRels(p->tableName);
			s.CopyRel(p->tableName, p->aliasAs);
			CreateTable(p->tableName);
			tableList.emplace(p->aliasAs, p->tableName);
			relNames.push_back(p->aliasAs);
			++numToJoin;
		}
		p = p->next;
	}
}

void PlanTree::CreateTable(char* tableName) {
	TableInfo tblInfo;
	tblInfo.sch = new Schema(catalog_path.c_str(), tableName);
	string file_path(dbf_prefix + std::string(tableName)+".bin");
	tblInfo.dbf = new DBFile();
	tblInfo.dbf->Create(file_path.c_str(),heap, 0);	
	// string load_path(tbl_prefix + std::string(tableName)+".tbl");
	// tblInfo.dbf->Load (*tblInfo.sch, load_path.c_str());	
	Attribute *atts = tblInfo.sch->GetAtts();
	InitDefaultPredicate(atts[0].name, tblInfo.initPred);
	tableInfo.emplace(tableName, std::move(tblInfo));
}

void PlanTree::SeparatePredicate(struct AndList *pAnd) {
	// remove relation name prefix in the predicate 
	RemovePrefix(pAnd);
	while (pAnd != nullptr) {
		bool containJoin = false;	
		bool containCrossSelect = false;	
		char* oldname = "";
		Predicate p = nullptr;
		// store the relations of cross selection, 0 means not, crossRelNo[i] = 1 means relNames[i] is contained in the Predicate
		// e.g. crossRelNo = {0, 1, 1} means cross selection contain relNames[1], relNames[2]
		std::vector<int> crossRelNo(numToJoin, 0);
		struct OrList *pOr = pAnd->left;
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			int op = pCom->code;
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME op NAME  (NAME = 4, thus NAME | NAME = 4)
			if (NAME == lOperand && NAME ==rOperand) {
				p = pAnd;
				std::string lname(pCom->left->value);				
				std::string rname(pCom->right->value);			
				//find relation ID of left and right attribute
				int left = s.FindAtts(&relNames[0], lname, numToJoin);
				int right = s.FindAtts(&relNames[0], rname, numToJoin);	
				//push the join infomation into joinList
				joinList.push_back(std::move(JoinRelInfo{left,right,p}));
				containJoin = true;
			}
			// NAME op value | value op name (NAME = 4 other = [1, 3], thus NAME | other > 4)
			else if (NAME == lOperand || NAME == rOperand) {
				if (true == containJoin) {
					cerr << "ERROR: Join and selection in one AND";
					exit(1);
				}
				p = pAnd;
				std::string name = s.InitAttsName(pCom);
				int i = s.FindAtts(&relNames[0], name, numToJoin);
				// set the corresponding relation slot in crossRelNo to 1
				crossRelNo[i] = 1;
				// if the oldname is empty, set to the first met relation
				if (!strcmp(oldname,"")) {
					oldname = relNames[i];		
				} else {
					//if conatin more than one relation in a selection Predicate
					//e.g. (l.l_orderkey < 100 OR o.o_orderkey < 100) 
					if(strcmp(oldname, relNames[i])) {
						containCrossSelect = true;
					}
				}						
			} 
			pOr = pOr->rightOr;
		}		
		if(!containJoin) {	
			// if not contain cross select, the selection can be pushed down to bottom
			// add it to non-cross selectList
			if (!containCrossSelect) {				
				auto ibp = selectList.emplace(oldname, p);
				if(false == ibp.second) {
					selectList.at(oldname)->rightAnd = p;
				}
			} else {
				// otherwise the Predicate can not be pushed down to bottom, put into cross selection list
				crossSelectList.push_back(std::move(CrossSelectInfo{std::move(crossRelNo), p}));
			}			
		}
		pAnd = pAnd->rightAnd;
		p->rightAnd = nullptr;
	}
}

void PlanTree::GetJoinOrder(struct AndList *pAnd) {	
	// check if the sum predicate is legal
	CheckSumPredicate(finalFunction, groupingAtts, attsToSelect);
	SeparatePredicate(pAnd);
	int numOfRels = 1;
	// store the all select file nodes
	std::vector<PlanNode*> nodeList;
	nodeList.reserve(numToJoin);
	GrowSelectFileNode(nodeList);
	// s.Print();
	if (numToJoin < 2) {
		return;
	}
	++numOfRels;
	// a hash table stores the relations have been joined
	// joinedTable[i] = 1 means relName[i] has been joined,  0 means not been joined	
	std::vector<int> joinedTable(numToJoin, 0);
	// minimun cost relation sequence
	std::vector<char*> minList;
	//use greedy algorithm, find the minimum cost join and apply
	GrowRowJoinNode(joinedTable, minList, nodeList);
	// find if any cross selection can be performed
	GrowSelectPipeNode(joinedTable, minList, numOfRels);
	// s.Print();
	++numOfRels;
	// keep finding minimum cost join and apply while joinList is not empty 
	while(!joinList.empty()) {		
		GrowCookedJoinNode(joinedTable, minList, nodeList, numOfRels);
		// find if any cross selection can be performed
		GrowSelectPipeNode(joinedTable, minList, numOfRels);		
		// s.Print();
		// increment number of relations
		++numOfRels;
	}

	if (groupingAtts && finalFunction) {
		if (distinctFunc) {
			CheckDistinctFunc(finalFunction, groupingAtts);
			GrowProjectNode(groupingAtts);
			GrowDuplicateRemovalNode();
		}
		GrowGroupByNode(finalFunction, groupingAtts);
	} else if (finalFunction) {
		if (distinctFunc) {			
			GrowDuplicateRemovalNode();
		}
		GrowSumNode(finalFunction);
	} else if (groupingAtts) {
		GrowProjectNode(groupingAtts);
		GrowDuplicateRemovalNode();
	}

	if (attsToSelect) {
		GrowProjectNode(attsToSelect);
	}

	if (distinctAtts) {
	 	GrowDuplicateRemovalNode();
	}

	
	// distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
	// if(writeOutFile) {
	// 	GrowWriteOutNode(writeOutFile);
	// }	
	root = bulidedNodes[bulidedNodes.size() - 1];
	PrintTree();
}

void PlanTree::GrowSelectFileNode(std::vector<PlanNode*> &nodeList) {
	for (int i = 0; i < numToJoin; ++i) {
		TableInfo &ti = tableInfo.at(tableList.at(relNames[i]));
		Record *rec = new Record();
		CNF *cnf = new CNF();
		SelectFileNode *treeNode = nullptr;
		auto it = selectList.find(relNames[i]);
		// if there is a select on the relation, grow cnf using the selection predicate
		if( it != selectList.end()) {
			char *rels[] = { (char*) it->first.c_str() };
			struct AndList *p = it->second;			
			cnf->GrowFromParseTree (p, ti.sch, *rec);
			treeNode = new SelectFileNode(ti.dbf, cnf, rec);
			treeNode->numTuples = s.Estimate(p, rels , 1);
			s.Apply(p, rels , 1);
		} else {
			// otherwise grow cnf using default predicate (R.a = R.a) store in tableInfo
			cnf->GrowFromParseTree(ti.initPred, ti.sch, *rec);
			treeNode = new SelectFileNode(ti.dbf, cnf, rec);
			treeNode->numTuples = s.GetNumTuples(tableList.at(relNames[i]));
		}
		treeNode->outSch = ti.sch;
		treeNode->type = Unary;
		nodeList.push_back(treeNode);
	}
}

void PlanTree::GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels) {
	int predNo;
	// check if any cross selection predicate can be apply
	while ( (predNo = CheckCrossSelect(crossSelectList, joinedTable)) != -1) {		
		Record *rec = new Record();
		CNF *cnf = new CNF();	
		int current = bulidedNodes.size() - 1;
		// grow cnf using the cross selection predicate
		cnf->GrowFromParseTree (crossSelectList[predNo].p, bulidedNodes[current]->outSch, *rec);
		SelectPipeNode *treeNode = new SelectPipeNode(cnf, rec);
		treeNode->outSch = bulidedNodes[current]->outSch;
		treeNode->numTuples = s.Estimate(crossSelectList[predNo].p, &minList[0] , numOfRels);
		BuildUnaryNode(bulidedNodes[current], treeNode);
		bulidedNodes.push_back(treeNode);
		s.Apply(crossSelectList[predNo].p, &minList[0] , numOfRels);
		crossSelectList.erase(crossSelectList.begin() + predNo);
	}
}

// check if a cross selection Predicate
int PlanTree::CheckCrossSelect(std::vector<CrossSelectInfo> &csl, std::vector<int> &joinedTable) {
	for (int i = 0; i < csl.size(); i++) {		
		std::vector<int> &relNo = csl[i].relNo;
		int j;
		for (j = 0; j < numToJoin; j++) {
			// if relNo[j] = 1 and joinedTable[j] = 0, the cross selection Predicate can't be perform yet
			if(relNo[j] && !joinedTable[j]) {
				break;
			}
		}	
		if (j == numToJoin) {
			return i;
		}
	}
	return -1;
}

void PlanTree::GrowRowJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<PlanNode*> &nodeList) {
	double min = std::numeric_limits<double>::max();
	auto minIt = joinList.begin();
	// traverse predicate set to find the join predicate with minimum cost
	for (auto it = joinList.begin(); it < joinList.end(); ++it) {
		char *rels[] = {relNames[it->left], relNames[it->right]};
		double result = s.Estimate(it->p, rels , 2);
		if (result < min) {
			min = result;			
			minIt = it;
		}
	}
	//****put the smaller relation on the left, use array to eliminate branch****
	int minIDs[] = { minIt->left, minIt->right };
	double leftNumTuples = s.GetNumTuples(tableList.at(relNames[minIt->left]));
	double rightNumTuples = s.GetNumTuples(tableList.at(relNames[minIt->right]));
	int leftID = minIDs[leftNumTuples > rightNumTuples];
	int rightID = minIDs[leftNumTuples <= rightNumTuples];

	// update joined relation name list
	minList.push_back(relNames[leftID]);
	minList.push_back(relNames[rightID]);
	// update joined table

	joinedTable[leftID] = 1;
	joinedTable[rightID] = 1;
	Record *rec = new Record();
	CNF *cnf = new CNF();
	TableInfo &lti = tableInfo.at(tableList.at(relNames[leftID]));
	TableInfo &rti = tableInfo.at(tableList.at(relNames[rightID]));
	cnf->GrowFromParseTree (minIt->p, lti.sch, rti.sch, *rec);

	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch = new Schema(*lti.sch, *rti.sch);
	treeNode->numTuples = s.Estimate(minIt->p, &minList[0] , 2);
	// initial first 2 output pipe
	nodeList[leftID]->outPipeID = 1;
	nodeList[rightID]->outPipeID = 2;
	BuildBinaryNode(nodeList[leftID], nodeList[rightID], treeNode, 3);
	bulidedNodes.push_back(treeNode);

	// apply the join predicate and erase the predicate in join list
	s.Apply(minIt->p, &minList[0] , 2);
	joinList.erase(minIt);
}

void PlanTree::GrowCookedJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<PlanNode*> &nodeList, int numOfRels) {
	int product_cout = 0;
	double min = std::numeric_limits<double>::max();
	int minId = -1, newId = -1;
	std::vector<char*> rels = minList;
	auto minIt = joinList.begin();
	for (auto it = joinList.begin(); it < joinList.end(); ++it) {
		if (1 == joinedTable[it->left]) {
			rels.push_back(relNames[it->right]);
			newId = it->right;
		} else if(1 == joinedTable[it->right]) {
			rels.push_back(relNames[it->left]);
			newId = it->left;
		} else {				
			++product_cout;
			// if for all predicates in join list, both relations in the prediacte don't appear in join table
			// cross product is needed
			if (product_cout == joinList.size()) {
				cerr << "Product is needed!";
				exit(1);
			}
			continue;
		}		
		double result = s.Estimate(it->p, &rels[0] , numOfRels);
		if (result < min) {
			min = result;
			minIt = it;
			minId = newId;
		}
		rels.pop_back();
	}
	if (-1 == minId) {
		cerr << "ERROR:minid is negative";
		exit(1);
	}
	// update joined table and min list
	joinedTable[minId] = 1;
	minList.push_back(relNames[minId]);
	// build join node member
	Record *rec = new Record();
	CNF *cnf = new CNF();	
	int current = bulidedNodes.size() - 1;
	TableInfo &rti = tableInfo.at(tableList.at(relNames[minId]));
	cnf->GrowFromParseTree (minIt->p, bulidedNodes[current]->outSch, rti.sch, *rec);

	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch = new Schema(*(bulidedNodes[current]->outSch), *rti.sch);
	treeNode->numTuples = s.Estimate(minIt->p, &minList[0] , numOfRels);
	nodeList[minId]->outPipeID = bulidedNodes[current]->outPipeID + 1;
	//*****put the smaller relation on the left, use array to eliminate branch*****
	PlanNode *minNodes[] = { bulidedNodes[current], nodeList[minId] };
	double leftNumTuples = bulidedNodes[current]->numTuples;
	double rightNumTuples = nodeList[minId]->numTuples;
	PlanNode *lchild = minNodes[leftNumTuples > rightNumTuples];
	PlanNode *rchild = minNodes[leftNumTuples <= rightNumTuples];	

	BuildBinaryNode (lchild, rchild, treeNode, bulidedNodes[current]->outPipeID + 2);
	bulidedNodes.push_back(treeNode);
	// apply the join predicate and erase the predicate in join list
	s.Apply(minIt->p, &minList[0] , numOfRels);
	joinList.erase(minIt);
}

void PlanTree::GrowProjectNode (struct NameList *attsToSelect) {	
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	int numAttsOutput = 0;
	std::vector<int> indices;
	struct NameList *p = attsToSelect;
	while (p) {
		if (p->name) {
			string name(p->name);
			// get the attribute name after alias relation name prefix
			int index = inSch->Find((char*)name.substr(name.find(".") + 1).c_str());
			if (-1 == index) {				
				cerr << "Projection attribute not found! " <<  name << endl;
				exit(1);
			}
			indices.push_back(index);
			++numAttsOutput;
		} 		
		p = p->next;
	}
	// build attribute indices to keep and attributes to keep
	int *keepMe = new int[numAttsOutput];
	Attribute *inAtts = inSch->GetAtts();
	Attribute *atts = new Attribute[numAttsOutput];
	for(int i = 0; i < numAttsOutput; i++) {
		keepMe[i] = indices[i];
		atts[i].myType = inAtts[indices[i]].myType;
		atts[i].name = strdup(inAtts[indices[i]].name);
	}
	ProjectNode *treeNode = new ProjectNode(keepMe, inSch->GetNumAtts(), numAttsOutput);
	treeNode->outSch = new Schema ("project_sch", numAttsOutput, atts);
	treeNode->numTuples = bulidedNodes[current]->numTuples;
	BuildUnaryNode(bulidedNodes[current], treeNode);
	bulidedNodes.push_back(treeNode);
}

void PlanTree::GrowDuplicateRemovalNode () {
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	DuplicateRemovalNode *treeNode = new DuplicateRemovalNode(inSch);
	treeNode->outSch = inSch;
	// half number of tuples
	treeNode->numTuples = 0.5 * bulidedNodes[current]->numTuples;
	BuildUnaryNode(bulidedNodes[current], treeNode);
	bulidedNodes.push_back(treeNode);
}

Type PlanTree::GetSumType(Function *func) {
	if(1 == func->returnsInt) {
		return Int;
	} else {
		return Double;
	}
}

void PlanTree::GrowSumNode (struct FuncOperator *finalFunction) {
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	Function *func = new Function();
	// remove the relation name prefix in finalFunction
	RemovePrefix(finalFunction);
	// grow Function using the finalFunction and input schema
	func->GrowFromParseTree (finalFunction, *inSch);
	SumNode *treeNode = new SumNode(func);
	Attribute *atts = new Attribute;	
	atts->myType = GetSumType(func);
	atts->name = strdup("SUM");
	treeNode->outSch = new Schema ("sum_sch", 1, atts);
	treeNode->numTuples = 1;
	BuildUnaryNode(bulidedNodes[current], treeNode);
	bulidedNodes.push_back(treeNode);
}

void PlanTree::GrowGroupByNode (struct FuncOperator *finalFunction, struct NameList *groupingAtts) {
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	struct NameList *p = groupingAtts;
	OrderMaker *om = new OrderMaker();
	int numAtts = 0;
	std::vector<int> indices;
	while (p) {
		if (p->name) {
			string name(p->name);
			// get the attribute name after alias relation name prefix
			int index = inSch->Find((char*)name.substr(name.find(".") + 1).c_str());
			if (-1 == index) {				
				cerr << "GroupBy attribute not found! " <<  name << endl;
				exit(1);
			}
			Type type = inSch->FindType(p->name);
			// add the group attribute into ordermaker
			om->Add(index, type);
			indices.push_back(index);
			++numAtts;
		} 		
		p = p->next;
	}
	// build the remaining attributes list
	Attribute *inAtts = inSch->GetAtts();
	Attribute *atts = new Attribute[numAtts + 1];
	for(int i = 0; i < numAtts; i++) {
		atts[i].myType = inAtts[indices[i]].myType;
		atts[i].name = strdup(inAtts[indices[i]].name);
	}
	GroupByNode *treeNode;
	if(finalFunction) {
		Function *func = new Function();
		// remove the relation name prefix in finalFunction
		RemovePrefix(finalFunction);
		func->GrowFromParseTree (finalFunction, *inSch);
		atts[numAtts].myType = GetSumType(func);
		atts[numAtts].name = strdup("SUM");
		treeNode = new GroupByNode(om, func);
		treeNode->outSch = new Schema ("group_sch", numAtts + 1, atts);
	} else {
		treeNode = new GroupByNode(om, nullptr);
		treeNode->outSch = new Schema ("group_sch", numAtts, atts);
	}	
	// half the number of tuples
	treeNode->numTuples = 0.5 * bulidedNodes[current]->numTuples;
	BuildUnaryNode(bulidedNodes[current], treeNode);
	bulidedNodes.push_back(treeNode);
}

void PlanTree::GrowWriteOutNode(const char* filename) {
	int current = bulidedNodes.size() - 1;
	Schema *inSch = bulidedNodes[current]->outSch;
	FILE *writefile = fopen (filename, "w");
	WriteOutNode *treeNode = new WriteOutNode(writefile, inSch, filename);
	treeNode->outSch = inSch;
	treeNode->numTuples = bulidedNodes[current]->numTuples;
	BuildUnaryNode(bulidedNodes[current], treeNode);
	bulidedNodes.push_back(treeNode);
}

// consturct family for unary node
void PlanTree::BuildUnaryNode(PlanNode *child, PlanNode *parent) {	
	parent->type = Unary;	
	parent->child = child;
	child->parent = parent;
	parent->inPipeID = child->outPipeID;
	parent->outPipeID = child->outPipeID + 1;
	if(parent->numTuples < 1) {
		parent->numTuples = 1;
	}
}

// consturct family for binary node
void PlanTree::BuildBinaryNode (PlanNode *lchild, PlanNode *rchild, JoinNode *parent, int outID) {
	parent->type = Binary;
	parent->child = lchild;
	parent->rchild = rchild;
	lchild->parent = parent;
	rchild->parent = parent;
	parent->inPipeID = lchild->outPipeID;
	parent->rightInPipeID = rchild->outPipeID;
	parent->outPipeID = outID;
	if(parent->numTuples < 1) {
		parent->numTuples = 1;
	}
}

// inorder print the plan tree
void PlanTree::PrintTree () {
	if (!root) {
		cerr << "The plan tree is empty!" << endl;
		exit(1);
	}
	PrintNode(root);
}

// recursively print the plan node
void PlanTree::PrintNode (PlanNode *treeNode) {
	if (!treeNode) {
		return;
	}
	PrintNode(treeNode->child);
	treeNode->Print();
	if(Binary == treeNode->type) {
		PrintNode(dynamic_cast<JoinNode*>(treeNode)->rchild);
	}	
}

void SelectFileNode::Print() {
	cout << "********\nSelect File Operation" << endl;
	cout << "Input from File " << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();	
	cout << "Select Pipe CNF: " << endl;
	selOp->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void SelectPipeNode::Print() {
	cout << "********\nSelect Pipe Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();	
	cout << "Select Pipe CNF: " << endl;
	selOp->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void ProjectNode::Print() { 
	cout << "********\nProject Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();	
	cout << "Attributes to keep: " << endl;
	cout << "{ ";
	for(int i = 0; i < numAttsOutput; i++) {
		cout << keepMe[i] << " ";
	}
	cout << "}\n";
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void JoinNode::Print() { 
	cout << "********\nJoin Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Input pipe ID " << rightInPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();	
	cout << "Join CNF: " << endl;
	selOp->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void DuplicateRemovalNode::Print() {
	cout << "********\nDuplicate Removal Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void SumNode::Print() {
	cout << "********\nSum Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();	
	cout << "Sum Function:" << endl;
	computeMe->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void GroupByNode::Print() {
	cout << "********\nGroupBy Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output pipe ID " << outPipeID << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();
	cout << "GroupBy OrderMaker:" << endl;
	groupAtts->Print();
	cout << "GroupBy Function:" << endl;
	computeMe->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" <<endl;
}

void WriteOutNode::Print() {
	cout << "********\nWriteOut Operation" << endl;
	cout << "Input pipe ID " << inPipeID << endl;
	cout << "Output file name" << filename << endl;
	cout << "Output Schema:" << endl;
	outSch->Print();
	cout << "Estimate number of tuples: " << numTuples << endl;
	cout << "********\n" << endl;
}
	
	