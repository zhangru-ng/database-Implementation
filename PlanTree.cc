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

void PlanTree::InitPredicate(char *attName, Predicate &initPred) {
	struct Operand *lOperand = (struct Operand *) malloc (sizeof (struct Operand));
	lOperand->value = attName;
	lOperand->code = NAME;
	struct Operand *rOperand = (struct Operand *) malloc (sizeof (struct Operand));
	rOperand->value = attName;
	rOperand->code = NAME;
	struct ComparisonOp *pCom  = (struct ComparisonOp *)malloc (sizeof (struct ComparisonOp));
	pCom->left = lOperand;
	pCom->code = EQUALS;
	pCom->right = rOperand;
	struct OrList *pOr = (struct OrList *) malloc (sizeof (struct OrList));
	pOr->left = pCom;
	pOr->rightOr = NULL;
	struct AndList *pAnd = (struct AndList *) malloc (sizeof (struct AndList));
	pAnd->left = pOr;
	pAnd->rightAnd = NULL;
	initPred = pAnd;
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
	InitPredicate(atts[0].name, tblInfo.initPred);
	tableInfo.emplace(tableName, std::move(tblInfo));
}

void PlanTree::SeparatePredicate(struct AndList *pAnd) {
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
	// a hash table stores the relations have been joined
	// joinedTable[i] = 1 means relName[i] has been joined,  0 means not been joined	
	SeparatePredicate(pAnd);
	int numOfRels = 1;
	std::vector<PlanNode*> nodeList;
	nodeList.reserve(numToJoin);
	GrowSelectFileNode(nodeList);
	s.Print();
	if (numToJoin < 2) {
		return;
	}
	++numOfRels;
	std::vector<int> joinedTable(numToJoin, 0);
	// minimun cost relation sequence
	std::vector<char*> minList;
	//use greedy algorithm, find the minimum cost join and apply
	GrowRowJoinNode(joinedTable, minList, nodeList);
	// find if any cross selection can be performed
	GrowSelectPipeNode(joinedTable, minList, numOfRels);
	s.Print();
	++numOfRels;
	// keep finding minimum cost join and apply while joinList is not empty 
	while(!joinList.empty()) {		
		GrowCookedJoinNode(joinedTable, minList, nodeList, numOfRels);
		// find if any cross selection can be performed
		GrowSelectPipeNode(joinedTable, minList, numOfRels);		
		s.Print();
		// increment number of relations
		++numOfRels;
	}

	if (groupingAtts && finalFunction) {
		GrowGroupByNode(finalFunction, groupingAtts);
	} else if(finalFunction) {
		GrowSumNode(finalFunction);
	}

	if (distinctAtts) {
	 	GrowDuplicateRemovalNode();
	}

	if (attsToSelect) {
		GrowProjectNode(attsToSelect);
	}
	// distinctFunc;  // 1 if there is a DISTINCT in an aggregate query


	
	root = bulidedNodes[bulidedNodes.size() - 1];
}

void PlanTree::GrowSelectFileNode(std::vector<PlanNode*> &nodeList) {
	for (int i = 0; i < numToJoin; ++i) {
		TableInfo &ti = tableInfo.at(tableList.at(relNames[i]));
		Record *rec = new Record();
		CNF *cnf = new CNF();
		SelectFileNode *treeNode = nullptr;
		auto it = selectList.find(relNames[i]);
		if( it != selectList.end()) {
			char *rels[] = { it->first };
			struct AndList *p = it->second;
			s.Apply(p, rels , 1);
			cnf->GrowFromParseTree (p, ti.sch, *rec);
			treeNode = new SelectFileNode(ti.dbf, cnf, rec);
		} else {
			cnf->GrowFromParseTree(ti.initPred, ti.sch, *rec);
			treeNode = new SelectFileNode(ti.dbf, cnf, rec);
		}
		treeNode->outSch = ti.sch;
		treeNode->type = Unary;
		nodeList.push_back(treeNode);
	}
}

void PlanTree::GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numOfRels) {
	int predNo;
	while ( (predNo = CheckCrossSelect(crossSelectList, joinedTable)) != -1) {
		s.Apply(crossSelectList[predNo].p, &minList[0] , numOfRels);
		Record *rec = new Record();
		CNF *cnf = new CNF();	
		int current = bulidedNodes.size() - 1;
		cnf->GrowFromParseTree (crossSelectList[predNo].p, bulidedNodes[current]->outSch, *rec);
		SelectPipeNode *treeNode = new SelectPipeNode(cnf, rec);		
		treeNode->child = bulidedNodes[current];
		bulidedNodes[current]->parent = treeNode;
		treeNode->outSch = bulidedNodes[current]->outSch;
		treeNode->type = Unary;
		bulidedNodes.push_back(treeNode);
		crossSelectList.erase(crossSelectList.begin() + predNo);
	}
}

void PlanTree::GrowRowJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<PlanNode*> &nodeList) {
	double min = std::numeric_limits<double>::max();
	auto minit = joinList.begin();
	// traverse predicate set to find the join predicate with minimum cost
	for (auto it = joinList.begin(); it < joinList.end(); ++it) {
		char *rels[] = {relNames[it->left], relNames[it->right]};
		double result = s.Estimate(it->p, rels , 2);
		if (result < min) {
			min = result;			
			minit = it;
		}
	}
	// update joined relation name list
	minList.push_back(relNames[minit->left]);
	minList.push_back(relNames[minit->right]);
	// update joined table
	joinedTable[minit->left] = 1;
	joinedTable[minit->right] = 1;
	Record *rec = new Record();
	CNF *cnf = new CNF();
	TableInfo &lti = tableInfo.at(tableList.at(relNames[minit->left]));
	TableInfo &rti = tableInfo.at(tableList.at(relNames[minit->right]));
	cnf->GrowFromParseTree (minit->p, lti.sch, rti.sch, *rec);

	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch = new Schema(*lti.sch, *rti.sch);
	treeNode->type = Binary;
	treeNode->child = nodeList[minit->left];
	treeNode->rchild = nodeList[minit->right];
	nodeList[minit->left]->parent = treeNode;
	nodeList[minit->right]->parent = treeNode;	
	bulidedNodes.push_back(treeNode);

	s.Apply(minit->p, &minList[0] , 2);
	joinList.erase(minit);
}

void PlanTree::GrowCookedJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<PlanNode*> &nodeList, int numOfRels) {
	int product_cout = 0;
	double min = std::numeric_limits<double>::max();
	int minId = -1, newId = -1;
	std::vector<char*> rels = minList;
	auto minit = joinList.begin();
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
			minit = it;
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
	cnf->GrowFromParseTree (minit->p, bulidedNodes[current]->outSch, rti.sch, *rec);

	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch = new Schema(*(bulidedNodes[current]->outSch), *rti.sch);
	treeNode->type = Binary;
	treeNode->child = bulidedNodes[current];
	treeNode->rchild = nodeList[minId];
	bulidedNodes[current]->parent = treeNode;
	nodeList[minId]->parent = treeNode;
	bulidedNodes.push_back(treeNode);

	s.Apply(minit->p, &minList[0] , numOfRels);
	joinList.erase(minit);
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
	treeNode->type = Unary;
	
	treeNode->child = bulidedNodes[current];
	bulidedNodes[current]->parent = treeNode;
	bulidedNodes.push_back(treeNode);
}

void PlanTree::GrowDuplicateRemovalNode () {
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	DuplicateRemovalNode *treeNode = new DuplicateRemovalNode(inSch);
	treeNode->outSch = inSch;
	treeNode->type = Unary;
	treeNode->child = bulidedNodes[current];
	bulidedNodes[current]->parent = treeNode;
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
	RemovePrefix(finalFunction);
	func->GrowFromParseTree (finalFunction, *inSch);
	SumNode *treeNode = new SumNode(func);
	Attribute *atts = new Attribute;	
	atts->myType = GetSumType(func);
	atts->name = strdup("SUM");
	treeNode->outSch = new Schema ("sum_sch", 1, atts);
	treeNode->type = Unary;
	treeNode->child = bulidedNodes[current];
	bulidedNodes[current]->parent = treeNode;
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
			om->Add(index, type);
			indices.push_back(index);
			++numAtts;
		} 		
		p = p->next;
	}
	Attribute *inAtts = inSch->GetAtts();
	Attribute *atts = new Attribute[numAtts + 1];
	for(int i = 0; i < numAtts; i++) {
		atts[i].myType = inAtts[indices[i]].myType;
		atts[i].name = strdup(inAtts[indices[i]].name);
	}
	Function *func = new Function();
	RemovePrefix(finalFunction);
	func->GrowFromParseTree (finalFunction, *inSch);
	atts[numAtts].myType = GetSumType(func);
	atts[numAtts].name = strdup("SUM");
	GroupByNode *treeNode = new GroupByNode(om, func);
	treeNode->outSch = new Schema ("group_sch", numAtts + 1, atts);
	treeNode->type = Unary;
	treeNode->child = bulidedNodes[current];
	bulidedNodes[current]->parent = treeNode;
	bulidedNodes.push_back(treeNode);
}

void PlanTree::GrowWriteOutNode(const char* filename) {
	int current = bulidedNodes.size() - 1;	
	Schema *inSch = bulidedNodes[current]->outSch;
	FILE *writefile = fopen (filename, "w");
	WriteOutNode *treeNode = new WriteOutNode(writefile, inSch);
	treeNode->outSch = inSch;
	treeNode->type = Unary;
	treeNode->child = bulidedNodes[current];
	bulidedNodes[current]->parent = treeNode;
	bulidedNodes.push_back(treeNode);
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