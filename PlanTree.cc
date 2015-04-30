#include "PlanTree.h"

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
extern char* outfileName;

std::vector<Pipe *> PlanNode::pipePool;

PlanTree::PlanTree(Statistics &s, Tables &tbl, int om) : root(nullptr), numOfRels(0), outMode(om), stat(s), table(tbl) { }

PlanTree::~PlanTree() {
	root->ClearPipePool();
	delete root;
}

int PlanTree::BuildTableList() {
	struct TableList *p = tables;
	int optimzie_flag = OPTIMIZED_ON;
	while (p) {		
		if(NOTFOUND == CheckRels(p->tableName)) {
			cerr << "Table is no exist in Database currently!\n";
			stat.ClearRel(relNames);
			return DISCARD;
		}
		if(false == table.tableInfo.at(p->tableName).loaded) {
			cerr << "Table " << p->tableName << " is not loaded! Please Load the table." << endl;
			stat.ClearRel(relNames);
			return DISCARD;
		}
		if (OPTIMIZED_ON == optimzie_flag && NOTFOUND == stat.CheckRels(p->tableName)) {
			cerr << "Current statistics does not contain " << p->tableName << ". " 
				 << "Can not perform query optimization." << endl;
			while(true) {
				cout << "Do you still want to run this query? (y or n) ";
				char key;
				cin >> key;
				if (key == 'y') {
					stat.ClearRel(relNames);					
					optimzie_flag = OPTIMIZED_OFF;
					break;
				} else if (key == 'n'){
					stat.ClearRel(relNames);
					return DISCARD;
				} 
			}	
		} 
		tableList.emplace(p->aliasAs, p->tableName);
		relNames.push_back(p->aliasAs);
		if (OPTIMIZED_ON == optimzie_flag) {
			stat.CopyRel(p->tableName, p->aliasAs);	
		}		
		++numOfRels;
		p = p->next;
	}	
	return optimzie_flag;
}

int PlanTree::CheckRels(const char* relName) {
	if(table.tableInfo.find(relName) == table.tableInfo.end()) {
		return NOTFOUND;
	}
	return FOUND;
}

int PlanTree::SeparatePredicate() {
	// remove relation name prefix in the predicate 
	while (boolean != nullptr) {
		bool containSelect = false;
		bool containJoin = false;	
		bool containCrossPred = false;
		string oldname("");
		int left = NOTFOUND;
		int right = NOTFOUND;
		Predicate p = boolean;
		// store the relations of cross selection, 0 means not, crossRelNo[i] = 1 means relNames[i] is contained in the Predicate
		// e.g. crossRelNo = {0, 1, 1} means cross selection contain relNames[1], relNames[2]
		std::vector<int> crossRelNo(numOfRels, 0);
		struct OrList *pOr = boolean->left;
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			int op = pCom->code;
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME op NAME  (NAME = 4, thus NAME | NAME = 4)
			if (NAME == lOperand && NAME ==rOperand) {
				if (containJoin || containSelect) {
					containCrossPred = true;
				}
				std::string lname(pCom->left->value);				
				std::string rname(pCom->right->value);				
				//find relation ID of left and right attribute
				left = FindAtts(relNames, lname);				
				right = FindAtts(relNames, rname);				
				if(NOTFOUND == left || NOTFOUND == right) {
					cerr << "ERROR: Query attribute dose not exist in table list!\n";
					return DISCARD;
				}
				crossRelNo[left] = 1;
				crossRelNo[right] = 1;
				// deal with an attribute join with itself, namely select all tuples in that relation
				if(left == right) {
					// containSelect = true;
					CheckCrossPred(oldname, left, containCrossPred);
					pOr = pOr->rightOr;
					continue;
				}
				// deal with non-equal join
				if (op != EQUALS) {
					containCrossPred = true;
					CheckCrossPred(oldname, left, containCrossPred);
					pOr = pOr->rightOr;
					continue;
				}
				containJoin = true;
			}
			// NAME op value | value op name (NAME = 4 other = [1, 3], thus NAME | other > 4)
			else if (NAME == lOperand || NAME == rOperand) {
				if (containJoin) {
					containCrossPred = true;
				}
				std::string name = stat.InitAttsName(pCom);
				// cout << name << endl;
				int i = FindAtts(relNames, name);
				if(NOTFOUND == i) {
					cerr << "ERROR: Query attribute dose not exist in table list!\n";
					return DISCARD;
				}
				containSelect = true;
				// set the corresponding relation slot in crossRelNo to 1
				crossRelNo[i] = 1;
				CheckCrossPred(oldname, i, containCrossPred);
			}
			pOr = pOr->rightOr;
		}
		/*****************************************************************************************/	
		// if contain cross relation predicate
		if (containCrossPred) {
			crossSelectList.push_back(std::move(CrossSelectInfo{std::move(crossRelNo), p})); 
		} 
		// if contain pure join
		else if (containJoin) {
			//push the join infomation into joinList
			joinList.push_back(std::move(JoinRelInfo{left,right,p}));
		} 
		// if contain pure selection
		else if (containSelect) {
			auto ibp = selectList.emplace(oldname, p);
			if(false == ibp.second) {
				Predicate curList = selectList.at(oldname);
				// find the rightmost and list
				while(curList->rightAnd) { curList = curList->rightAnd; }
				curList->rightAnd = p;
			}
		}
		/*****************************************************************************************/
		boolean = boolean->rightAnd;
		p->rightAnd = nullptr;
	}
}

int PlanTree::FindAtts(std::vector<char*> relNames, std::string &name) {
	int size = relNames.size();
	int pos = name.find(".");		
	string relName = name.substr(0, pos);
	for(int i = 0; i < size; ++i) {
		if(!relName.compare(relNames[i])) {
			return i;
		}
	}
	return NOTFOUND;
}

// check if a cross relation predicate exist in the input andlist
void PlanTree::CheckCrossPred(std::string &oldname, int relID, bool &containCrossPred) {
	// if the oldname is empty, set to the first met relation
	if (!oldname.compare("")) {
		oldname = relNames[relID];		
	} else {
		//if conatin more than one relation in a selection Predicate
		//e.g. (l.l_orderkey < 100 OR o.o_orderkey < 100) 
		if(oldname.compare(relNames[relID])) {
			containCrossPred = true;
		}
	}
}

void PlanTree::PrintSeparateResult() {
	cout << "SELECT list:" << endl;
	for(auto s : selectList) {
		print_AndList(s.second);
		cout << endl;
	}
	cout << endl;
	cout << "JOIN list:" << endl;
	for(auto j : joinList) {
		print_AndList(j.p);
		cout << endl;
	}
	cout << endl;
	cout << "CROSS list:" << endl;
	for(auto c : crossSelectList) {
		print_AndList(c.p);
		cout << endl;
	}
	cout << endl;
}

int PlanTree::GetPlanTree() {	
	int optimzie_flag = BuildTableList();
	if(DISCARD == optimzie_flag) {
		return DISCARD;
	}
	// check if the sum predicate is legal
	if(DISCARD == CheckSumPredicate(finalFunction, groupingAtts, attsToSelect)) {
		stat.ClearRel(relNames);
		return DISCARD;
	}
	if(DISCARD == SeparatePredicate()) {
		stat.ClearRel(relNames);
		return DISCARD;
	}
	if (numOfRels > joinList.size() + 1) {
		char ch;
		while (true) {
			cerr << "Cross product is needed, Do you still want to perform this query? (y or n)\n";
			cin >> ch;
			if(ch == 'y') {
				break;
			} else if (ch == 'n') {
				stat.ClearRel(relNames);
				return DISCARD;
			}
		}
	}
	// store the all select file nodes
	GrowSelectFileNode(optimzie_flag);
	if (1 == numOfRels) {
		buildedNodes.push_back(selectFileList[0]);
		buildedNodes[0]->outPipeID = 0;
	}
	// stat.Print();
	if (numOfRels > 1) {
		int numToJoin = 2;
		// a hash table stores the relations have been joined
		// joinedTable[i] = 1 means relName[i] has been joined,  0 means not been joined	
		std::vector<int> joinedTable(numOfRels, 0);
		// minimun cost relation sequence(use char* for compatibility with char * array)
		std::vector<char*> minList;
		// remaining not joined relations
		std::vector<int> remainList;
		//use greedy algorithm, find the minimum cost join and apply
		GrowRawJoinNode(joinedTable, minList, remainList, optimzie_flag);
		// find if any cross selection can be performed
		GrowSelectPipeNode(joinedTable, minList, numToJoin, optimzie_flag);
		// stat.Print();
		++numToJoin;
		// keep finding minimum cost join and apply while joinList is not empty 
		while(numToJoin <= numOfRels) {		
			GrowCookedJoinNode(joinedTable, minList, remainList, numToJoin, optimzie_flag);
			// find if any cross selection can be performed
			GrowSelectPipeNode(joinedTable, minList, numToJoin, optimzie_flag);		
			// stat.Print();
			// increment number of relations
			++numToJoin;
		}
	}
	// if both SUM and GROUP BY are on
	if (groupingAtts && finalFunction) {
		// if  DISTINCT on aggregation		
		if (distinctFunc) {
			// get the name contain group by attribute and sum attribute
			struct NameList *disFuncAtts = GetDistinctName(finalFunction, groupingAtts);
			// project on the attributes and remove duplicate before grouping
			GrowProjectNode(disFuncAtts);
			DestroyNameList(disFuncAtts);
			GrowDuplicateRemovalNode();
		}
		GrowGroupByNode(finalFunction, groupingAtts);
		if(!attsToSelect) {
			struct NameList *sum = BuildNameList("SUM");
			GrowProjectNode(sum);
			DestroyNameList(sum);
		} else if (CountGroupAndSelect(groupingAtts, attsToSelect)){
			BuildGroupSelect(attsToSelect);
			GrowProjectNode(attsToSelect);
		}	
	} 
	// else if only SUM is on
	else if (finalFunction) {
		if (distinctFunc) {
			struct NameList *sumAtts = GetSumAtts(finalFunction);			
			GrowProjectNode(sumAtts);			
			DestroyNameList(sumAtts);
			GrowDuplicateRemovalNode();
		}
		GrowSumNode(finalFunction);
	}
	// else if only GROUP BY is on
	else if (groupingAtts) {
		// equal to project on grouping attribute then remove duplicate
		GrowProjectNode(groupingAtts);
		GrowDuplicateRemovalNode();
	}
	// if the SELECT name list is not empty
	if (attsToSelect && !groupingAtts) {
		GrowProjectNode(attsToSelect);
	}
	// if DISTINCT on non-aggregate attribute
	if (distinctAtts) {
	 	GrowDuplicateRemovalNode();
	}

	// if SET output to file
	if(STDOUT_ == outMode) {
		GrowWriteOutNode("", outMode);
	} else if (OUTFILE_ == outMode) {
		GrowWriteOutNode(outfileName, outMode);
	}
	// set root pointer of the plan tree
	int current = buildedNodes.size() - 1;
	root = buildedNodes[current];
	return RESUME;
}

void PlanTree::Print() {
	PrintVisitor printVisitor;
	VisitTree(printVisitor);
}

void PlanTree::Execute() {	
	cout << "\nExecuting query...\n" << endl;
	int current = buildedNodes.size() - 1;
	root->InitPipePool(buildedNodes[current]->outPipeID + 1);	
	RunVisitor runVisitor;
	double begin = clock();
	VisitTree(runVisitor);
	Wait();
	double end = clock();
	double cpu_time_used = (end - begin) / CLOCKS_PER_SEC;
	cout << "Query spent " << cpu_time_used << endl;
	if(OUTFILE_ == outMode) {
		StoreResult();
	} else {
		stat.ClearRel(relNames);
	}	
}

void PlanTree::Wait() {
	WaitVisitor joinVisitor;
	VisitTree(joinVisitor);
}

void PlanTree::StoreResult() {	
	while (true) {
		cout << "Do you want to store the resulting table? (y or n) ";
		char ch;
		cin >> ch;		
		if ('y' == ch) {
			break;
		} else if ('n' == ch) {
			stat.ClearRel(relNames);
			return;
		}
	}
	cout << "Please input a new name for the resulting table " << endl;
	cout << ">>> ";
	std::string resultName;
	while (true) {
		cin >> resultName;		
		if (NOTFOUND == CheckRels(resultName.c_str()) && NOTFOUND == stat.CheckRels(resultName.c_str())) {
			break;
		}
		cout << "Name conflict, please input a new name" << endl;
		cout << ">>> ";
	}
	stat.ClearRel(resultName, relNames);
	table.Store(resultName, *(root->outSch));
}

void PlanTree::GrowSelectFileNode(int optimzie_flag) {
	for (int i = 0; i < numOfRels; ++i) {
		TableInfo &ti = table.tableInfo.at(tableList.at(relNames[i]));
		Record *rec = new Record();		
		SelectFileNode *treeNode = nullptr;
		auto it = selectList.find(relNames[i]);
		// if there is a select on the relation, grow cnf using the selection predicate
		if( it != selectList.end()) {
			char *rels[] = { (char*) it->first.c_str() };
			struct AndList *p = it->second;
			double result = UNKNOWN;
			if (OPTIMIZED_ON == optimzie_flag) {
				result = stat.Estimate(p, rels , 1);	
				stat.Apply(p, rels , 1);
			}			
			CNF *cnf = new CNF();
			RemovePrefix(p);
			cnf->GrowFromParseTree (p, &ti.sch, *rec);
			treeNode = new SelectFileNode(&ti.dbf, cnf, rec);
			treeNode->numTuples = result;
		} else {
			// otherwise grow cnf using default predicate (R.a = R.a) 
			CNF *initCNF = new CNF(ti.sch);			
			treeNode = new SelectFileNode(&ti.dbf, initCNF, rec);
			if (OPTIMIZED_ON == optimzie_flag) {
				treeNode->numTuples = stat.GetNumTuples(tableList.at(relNames[i]));
			} 	
		}
		treeNode->outSch = &ti.sch;
		treeNode->type = Unary;
		treeNode->dbf_path = ti.dbf_path;
		selectFileList.push_back(treeNode);
	}
}

void PlanTree::GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, int numToJoin, int optimzie_flag) {
	int predNo;
	// check if any cross selection predicate can be apply
	while ( (predNo = CheckCrossSelect(crossSelectList, joinedTable)) != -1) {
		double result = UNKNOWN;
		if (OPTIMIZED_ON == optimzie_flag) {
			result = stat.Estimate(crossSelectList[predNo].p, &minList[0] , numToJoin);
			stat.Apply(crossSelectList[predNo].p, &minList[0] , numToJoin);
		}
		Record *rec = new Record();
		CNF *cnf = new CNF();	
		int current = buildedNodes.size() - 1;
		// grow cnf using the cross selection predicate
		RemovePrefix(crossSelectList[predNo].p);
		cnf->GrowFromParseTree (crossSelectList[predNo].p, buildedNodes[current]->outSch, *rec);
		SelectPipeNode *treeNode = new SelectPipeNode(cnf, rec);
		treeNode->outSch = buildedNodes[current]->outSch;
		treeNode->numTuples = result;
		BuildUnaryNode(buildedNodes[current], treeNode);
		buildedNodes.push_back(treeNode);
		crossSelectList.erase(crossSelectList.begin() + predNo);
	}
}

// check if a cross selection Predicate
int PlanTree::CheckCrossSelect(std::vector<CrossSelectInfo> &csl, std::vector<int> &joinedTable) {
	for (int i = 0; i < csl.size(); i++) {		
		std::vector<int> &relNo = csl[i].relNo;
		int j;
		for (j = 0; j < numOfRels; j++) {
			// if relNo[j] = 1 and joinedTable[j] = 0, the cross selection Predicate can't be perform yet
			if(relNo[j] && !joinedTable[j]) {
				break;
			}
		}	
		if (j == numOfRels) {
			return i;
		}
	}
	return -1;
}

void PlanTree::GrowRawJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<int> &remainList, int optimzie_flag) {
	double min = std::numeric_limits<double>::max();
	// initialize the remaining relation list
	for(int i = 0; i < numOfRels; i++) {
		remainList.push_back(i);
	}
	int mini = 0;
	int minj = 0;
	auto minIt = joinList.end();
	if(OPTIMIZED_ON == optimzie_flag) {
		for(auto it1 = remainList.begin(); it1 != remainList.end() - 1; ++it1) {
			for(auto it2 = remainList.begin() + 1; it2 != remainList.end(); ++it2) {
				// traverse predicate set to find if any join predicate can be applied
				Predicate p = nullptr;
				auto itj = joinList.begin();
				for ( ; itj != joinList.end(); ++itj) {
					if ((itj->left == *it1 && itj->right == *it2) || (itj->left == *it2 && itj->right == *it1)) {
						p = itj->p;
						break;
					}
				}
				char *rels[] = {relNames[*it1], relNames[*it2]};
				// p = nullptr means cross product
				double result = stat.Estimate(p, rels , 2);
				if (result < min) {
					// store minimum infomation
					min = result;
					mini = *it1;
					minj = *it2;
					minIt = itj;
				}
			}
		}	
	} else {
		min = UNKNOWN;
		// Pick the first predicate in join list
		if (!joinList.empty()) {
			minIt = joinList.begin();
			mini = minIt->left < minIt->right ? minIt->left : minIt->right;
			minj = minIt->left > minIt->right ? minIt->left : minIt->right;
		}
		else {
			mini = 0;
			minj = 1;
		}
	}
	// update remaining list
	remainList.erase(remainList.begin() + minj);
	remainList.erase(remainList.begin() + mini);
	// update joined table
	joinedTable[mini] = 1;
	joinedTable[minj] = 1;

	// double leftNumTuples = stat.GetNumTuples(tableList.at(relNames[mini]));
	// double rightNumTuples = stat.GetNumTuples(tableList.at(relNames[minj]));
	// int smaller = leftNumTuples > rightNumTuples;
	// update joined relation name list
	minList = { relNames[mini], relNames[minj] };	
	TableInfo &lti = table.tableInfo.at(tableList.at(relNames[mini]));
	TableInfo &rti = table.tableInfo.at(tableList.at(relNames[minj]));	
	Predicate minPred = nullptr;
	if (minIt != joinList.end()) {
		minPred = minIt->p;
	}
	// apply the join predicate, must be applied before remove prefix
	if(OPTIMIZED_ON == optimzie_flag) {
		stat.Apply(minPred, &minList[0] , 2);
	}
	CNF *cnf = nullptr;
	Record *rec = new Record();	
	if(minPred)	{
		cnf = new CNF();
		RemovePrefix(minIt->p);
		cnf->GrowFromParseTree (minIt->p, &lti.sch, &rti.sch, *rec);
		joinList.erase(minIt);
	} 
	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch = new Schema(lti.sch, rti.sch);
	treeNode->numTuples = min;
	// initial first 2 output pipe
	selectFileList[mini]->outPipeID = 0;
	selectFileList[minj]->outPipeID = 1;
	BuildBinaryNode(selectFileList[mini], selectFileList[minj], treeNode, 2);
	buildedNodes.push_back(treeNode);	
}


void PlanTree::GrowCookedJoinNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<int> &remainList, int numToJoin, int optimzie_flag) {
	double min = std::numeric_limits<double>::max();
	int minId = -1;
	std::vector<char*> rels = minList;
	auto minIt = joinList.end();
	auto minItR = remainList.begin();
	if(OPTIMIZED_ON == optimzie_flag) {
		for(auto it = remainList.begin(); it != remainList.end(); ++it) {
			rels.push_back(relNames[*it]);
			Predicate p = nullptr;
			auto itj = joinList.begin();
			for ( ; itj < joinList.end(); ++itj) {		
				if ((itj->left == *it && 1 == joinedTable[itj->right]) || (1 == joinedTable[itj->left] && itj->right == *it)) {
					p = itj->p;	
					break;
				}
			}
			double result = stat.Estimate(p, &rels[0] , numToJoin);
			if (result < min) {
				// store minimum infomation
				minItR = it;
				minId = *it;
				min = result;
				minIt = itj;
			}
			rels.pop_back();
		}
	} else {
		min = UNKNOWN;
		int flag = NOTFOUND;
		for(auto it = remainList.begin(); it != remainList.end() && NOTFOUND == flag; ++it) {
			for (auto itj = joinList.begin(); itj < joinList.end(); ++itj) {		
				if ((itj->left == *it && 1 == joinedTable[itj->right]) || (1 == joinedTable[itj->left] && itj->right == *it)) {
					minIt = itj;
					minItR = it;
					minId = *it;
					flag = FOUND;
					break;
				}
			}
		}
		if (NOTFOUND == flag) {
			minItR = remainList.begin();
			minId = *minItR;
		}
	}	
	// update joined table, remaining list and min list
	joinedTable[minId] = 1;
	remainList.erase(minItR);
	minList.push_back(relNames[minId]);
	// build join node member	
	int current = buildedNodes.size() - 1;
	TableInfo &rti = table.tableInfo.at(tableList.at(relNames[minId]));
	Predicate minPred = nullptr;	
	if (minIt != joinList.end()) {
		minPred = minIt->p;
	}
	// apply the join predicate, must be applied before remove prefix
	if (OPTIMIZED_ON == optimzie_flag) {
		stat.Apply(minIt->p, &minList[0] , numToJoin);
	}
	CNF *cnf = nullptr;
	Record *rec = new Record();
	if (minPred) {
		cnf = new CNF();
		RemovePrefix(minIt->p);
		cnf->GrowFromParseTree (minIt->p, buildedNodes[current]->outSch, &rti.sch, *rec);
		// erase the predicate in join list
		joinList.erase(minIt);
	}	
	JoinNode *treeNode = new JoinNode(cnf, rec);
	treeNode->outSch =  new Schema(*(buildedNodes[current]->outSch), rti.sch);
	treeNode->numTuples = min;
	selectFileList[minId]->outPipeID = buildedNodes[current]->outPipeID + 1;

	// double leftNumTuples = buildedNodes[current]->numTuples;
	// double rightNumTuples = selectFileList[minId]->numTuples;
	// int smaller = leftNumTuples > rightNumTuples;

	PlanNode *lchild = buildedNodes[current];
	PlanNode *rchild = selectFileList[minId];

	BuildBinaryNode (lchild, rchild, treeNode, buildedNodes[current]->outPipeID + 2);
	buildedNodes.push_back(treeNode);
}

void PlanTree::GrowProjectNode (struct NameList *attsToSelect) {	
	int current = buildedNodes.size() - 1;	
	Schema* inSch = buildedNodes[current]->outSch;
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
		// namelist is produced from LR grammar, need to reverse the order
		int rindex = numAttsOutput - i - 1;
		keepMe[i] = indices[rindex];
		atts[i].myType = inAtts[indices[rindex]].myType;
		atts[i].name = inAtts[indices[rindex]].name;
	}
	ProjectNode *treeNode = new ProjectNode(keepMe, inSch->GetNumAtts(), numAttsOutput);
	treeNode->outSch =  new Schema("project_sch", numAttsOutput, atts);
	treeNode->numTuples = buildedNodes[current]->numTuples;
	BuildUnaryNode(buildedNodes[current], treeNode);
	buildedNodes.push_back(treeNode);
	delete[] atts;
}

void PlanTree::GrowDuplicateRemovalNode () {
	int current = buildedNodes.size() - 1;	
	Schema *inSch = buildedNodes[current]->outSch;
	DuplicateRemovalNode *treeNode = new DuplicateRemovalNode(inSch);
	treeNode->outSch = inSch;
	// half number of tuples
	treeNode->numTuples = 0.5 * buildedNodes[current]->numTuples;
	BuildUnaryNode(buildedNodes[current], treeNode);
	buildedNodes.push_back(treeNode);
}

Type PlanTree::GetSumType(Function *func) {
	if(1 == func->returnsInt) {
		return Int;
	} else {
		return Double;
	}
}

void PlanTree::GrowSumNode (struct FuncOperator *finalFunction) {
	int current = buildedNodes.size() - 1;	
	Schema *inSch = buildedNodes[current]->outSch;
	Function *func = new Function();
	// remove the relation name prefix in finalFunction
	RemovePrefix(finalFunction);
	// grow Function using the finalFunction and input schema
	func->GrowFromParseTree (finalFunction, *inSch);
	SumNode *treeNode = new SumNode(func);
	Attribute *atts = new Attribute;	
	atts->myType = GetSumType(func);
	atts->name = "SUM";
	treeNode->outSch = new Schema("sum_sch", 1, atts);
	treeNode->numTuples = 1;
	BuildUnaryNode(buildedNodes[current], treeNode);
	buildedNodes.push_back(treeNode);
	delete atts;
}

void PlanTree::GrowGroupByNode (struct FuncOperator *finalFunction, struct NameList *groupingAtts) {
	int current = buildedNodes.size() - 1;	
	Schema *inSch = buildedNodes[current]->outSch;
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
		// namelist is produced from LR grammar, need to reverse the order
		int rindex = numAtts - i - 1;
		atts[i + 1].myType = inAtts[indices[rindex]].myType;
		atts[i + 1].name = inAtts[indices[rindex]].name;
	}
	GroupByNode *treeNode;
	if(finalFunction) {
		Function *func = new Function();
		// remove the relation name prefix in finalFunction
		RemovePrefix(finalFunction);
		func->GrowFromParseTree (finalFunction, *inSch);
		atts[0].myType = GetSumType(func);
		atts[0].name = "SUM";
		treeNode = new GroupByNode(om, func);
		treeNode->outSch = new Schema("group_sch", numAtts + 1, atts);
	} else {
		treeNode = new GroupByNode(om, nullptr);
		treeNode->outSch = new Schema("group_sch", numAtts, atts);
	}	
	// half the number of tuples
	treeNode->numTuples = 0.5 * buildedNodes[current]->numTuples;
	BuildUnaryNode(buildedNodes[current], treeNode);
	buildedNodes.push_back(treeNode);
	delete[] atts;
}

void PlanTree::GrowWriteOutNode(const char* filename, int outputMode) {
	int current = buildedNodes.size() - 1;
	Schema *inSch = buildedNodes[current]->outSch;
	WriteOutNode *treeNode = nullptr;
	if(STDOUT_ == outputMode) {
		treeNode = new WriteOutNode(inSch, outputMode);
	} else if (OUTFILE_ == outputMode) {		
		treeNode = new WriteOutNode(nullptr, inSch, filename, outputMode);
	} 
	treeNode->outSch = inSch;
	treeNode->numTuples = buildedNodes[current]->numTuples;
	BuildUnaryNode(buildedNodes[current], treeNode);
	buildedNodes.push_back(treeNode);
}

// consturct family for unary node
void PlanTree::BuildUnaryNode(PlanNode *child, PlanNode *parent) {	
	parent->type = Unary;	
	parent->child = child;
	child->parent = parent;
	parent->inPipeID = child->outPipeID;
	parent->outPipeID = child->outPipeID + 1;
	if(parent->numTuples != UNKNOWN && parent->numTuples < 1) {
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
	if(parent->numTuples != UNKNOWN && parent->numTuples < 1) {
		parent->numTuples = 1;
	}
}

// inorder print the plan tree
void PlanTree::VisitTree(PlanNodeVisitor &v) {
	if (!root) {
		cerr << "The plan tree is empty!" << endl;
		exit(1);
	}
	VisitNode(root, v);
}

// recursively print the plan node
void PlanTree::VisitNode (PlanNode *treeNode, PlanNodeVisitor &v) {
	if (!treeNode) {
		return;
	}
	VisitNode(treeNode->child, v);
	treeNode->Visit(v);
	if(Binary == treeNode->type) {
		VisitNode(static_cast<JoinNode*>(treeNode)->rchild, v);
	}	
}

SelectFileNode::SelectFileNode(DBFile *dbf, CNF *cnf, Record *rec) : inFile(dbf), selOp(cnf), literal(rec) { }

SelectFileNode::~SelectFileNode() {
	delete selOp;		
	delete literal;	
	inFile->Close();
}

void SelectFileNode::Visit(PlanNodeVisitor &v) {
	v.VisitSelectFileNode(this);
}

void PrintVisitor::VisitSelectFileNode(SelectFileNode *node) {
	cout << "********************************\nSelect File Operation" << endl;
	cout << "Input from File " << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();	
	cout << "Select File CNF: " << endl;
	node->selOp->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitSelectFileNode(SelectFileNode *node) {
	node->inFile->Open(node->dbf_path.c_str());
	node->sf.Run(*(node->inFile), *(node->pipePool[node->outPipeID]), *(node->selOp), *(node->literal));
}

void WaitVisitor::VisitSelectFileNode(SelectFileNode *node) {
	node->sf.WaitUntilDone();
}


SelectPipeNode::SelectPipeNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec) { }

SelectPipeNode::~SelectPipeNode() {
	delete selOp;
	delete literal;		
}

void SelectPipeNode::Visit(PlanNodeVisitor &v) {
	v.VisitSelectPipeNode(this);
}

void PrintVisitor::VisitSelectPipeNode(SelectPipeNode *node) {
	cout << "********************************\nSelect Pipe Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();	
	cout << "Select Pipe CNF: " << endl;
	node->selOp->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitSelectPipeNode(SelectPipeNode *node) {
	node->sp.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->outPipeID]), *(node->selOp), *(node->literal));
}

void WaitVisitor::VisitSelectPipeNode(SelectPipeNode *node) {
	node->sp.WaitUntilDone();
}


ProjectNode::ProjectNode(int *atts, int numIn, int numOut) : keepMe(atts), numAttsInput(numIn), numAttsOutput(numOut) { }
	
ProjectNode::~ProjectNode() {
	delete[] keepMe;
	delete outSch;
}

void ProjectNode::Visit(PlanNodeVisitor &v) {
	v.VisitProjectNode(this);
}

void PrintVisitor::VisitProjectNode(ProjectNode *node) { 
	cout << "********************************\nProject Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();	
	cout << "Attributes to keep: " << endl;
	cout << "{ ";
	for(int i = 0; i < node->numAttsOutput; i++) {
		cout << node->keepMe[i] << " ";
	}
	cout << "}\n";
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}
	
void RunVisitor::VisitProjectNode(ProjectNode *node) { 
	node->pj.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->outPipeID]), node->keepMe, node->numAttsInput, node->numAttsOutput);
}

void WaitVisitor::VisitProjectNode(ProjectNode *node) { 
	node->pj.WaitUntilDone();
}


JoinNode::JoinNode(CNF *cnf, Record *rec) : selOp(cnf), literal(rec), rchild(nullptr), rightInPipeID(-1) { }
	
JoinNode::~JoinNode() {
	delete selOp;
	delete literal;	
	delete outSch;
	if(rchild) {
		delete rchild;
		rchild = nullptr;
	}
}

void JoinNode::Visit(PlanNodeVisitor &v) {
	v.VisitJoinNode(this);
}

void PrintVisitor::VisitJoinNode(JoinNode *node) { 
	cout << "********************************\nJoin Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Input pipe ID " << node->rightInPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();	
	cout << "Join CNF: " << endl;
	if (node->selOp) {
		node->selOp->Print();
	} else {
		cout << "Cross product" << endl;
	}	
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitJoinNode(JoinNode *node) { 
	node->jn.Use_n_Pages(node->pagenum);
	node->jn.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->rightInPipeID]), *(node->pipePool[node->outPipeID]), *(node->selOp), *(node->literal));
}

void WaitVisitor::VisitJoinNode(JoinNode *node) { 
	node->jn.WaitUntilDone();
}

DuplicateRemovalNode::DuplicateRemovalNode(Schema * sch) : mySchema(sch) { }

DuplicateRemovalNode::~DuplicateRemovalNode() {
	// nothing to do
}

void DuplicateRemovalNode::Visit(PlanNodeVisitor &v) {
	v.VisitDuplicateRemovalNode(this);
}

void PrintVisitor::VisitDuplicateRemovalNode(DuplicateRemovalNode *node) {
	cout << "********************************\nDuplicate Removal Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitDuplicateRemovalNode(DuplicateRemovalNode *node) {
	node->dr.Use_n_Pages(node->pagenum);
	node->dr.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->outPipeID]), *(node->mySchema));
}

void WaitVisitor::VisitDuplicateRemovalNode(DuplicateRemovalNode *node) {
	node->dr.WaitUntilDone();
}


SumNode::SumNode(Function *func) : computeMe(func) { }

SumNode::~SumNode() {
	delete computeMe;
	delete outSch;
}

void SumNode::Visit(PlanNodeVisitor &v) {
	v.VisitSumNode(this);
}

void PrintVisitor::VisitSumNode(SumNode *node) {
	cout << "********************************\nSum Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();	
	cout << "Sum Function:" << endl;
	node->computeMe->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitSumNode(SumNode *node) {
	node->sm.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->outPipeID]), *(node->computeMe));
}

void WaitVisitor::VisitSumNode(SumNode *node) {
	node->sm.WaitUntilDone();
}


GroupByNode::GroupByNode(OrderMaker *om, Function *func) : groupAtts(om), computeMe(func) { }	

GroupByNode::~GroupByNode() {
	delete groupAtts;
	delete computeMe;
	delete outSch;
}
	
void GroupByNode::Visit(PlanNodeVisitor &v) {
	v.VisitGroupByNode(this);
}

void PrintVisitor::VisitGroupByNode(GroupByNode *node) {
	cout << "********************************\nGroupBy Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output pipe ID " << node->outPipeID << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();
	cout << "GroupBy OrderMaker:" << endl;
	node->groupAtts->Print();
	cout << "GroupBy Function:" << endl;
	node->computeMe->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" <<endl;
}

void RunVisitor::VisitGroupByNode(GroupByNode *node) {
	node->gb.Use_n_Pages(node->pagenum);
	node->gb.Run(*(node->pipePool[node->inPipeID]), *(node->pipePool[node->outPipeID]), *node->groupAtts, *(node->computeMe));
}

void WaitVisitor::VisitGroupByNode(GroupByNode *node) {
	node->gb.WaitUntilDone();
}


WriteOutNode::WriteOutNode(Schema *sch, int mode) : mySchema(sch), outputMode(mode) { }

WriteOutNode::WriteOutNode(FILE *f, Schema *sch, std::string name, int mode) : outFile(f), mySchema(sch), filename(name), outputMode(mode) { }


WriteOutNode::~WriteOutNode() {
	// nothing to do
}

void WriteOutNode::Visit(PlanNodeVisitor &v) {
	v.VisitWriteOutNode(this);
}

void PrintVisitor::VisitWriteOutNode(WriteOutNode *node) {
	cout << "********************************\nWriteOut Operation" << endl;
	cout << "Input pipe ID " << node->inPipeID << endl;
	cout << "Output file name: " << node->filename << endl;
	cout << "Output Schema:" << endl;
	node->outSch->Print();
	cout << "Estimate number of tuples: ";
	if (UNKNOWN == node->numTuples) {
		cout << "UNKNOWN" << endl;
	} else {
		cout << node->numTuples << endl;
	}	
	cout << "********************************\n" << endl;
}

void RunVisitor::VisitWriteOutNode(WriteOutNode *node) {
	if (STDOUT_ == node->outputMode) {
		node->wo.Run(*(node->pipePool[node->inPipeID]), *(node->mySchema), node->outputMode);
	} else if (OUTFILE_ == node->outputMode) {
		node->outFile = fopen(node->filename.c_str(), "w");
		node->wo.Run(*(node->pipePool[node->inPipeID]), node->outFile, *(node->mySchema), node->outputMode);
	}	
}

void WaitVisitor::VisitWriteOutNode(WriteOutNode *node) {
	node->wo.WaitUntilDone ();
}

PlanNode::PlanNode() : type(Unary), parent(nullptr), child(nullptr), inPipeID(-1), outPipeID(-1), outSch(nullptr), numTuples(UNKNOWN) { }

PlanNode::~PlanNode() {
	if(child) {
		delete child;
		child = nullptr;
	}
}

void PlanNode::InitPipePool(int poolSize) {
	pipePool.reserve(poolSize);
	for(int i = 0; i < poolSize; ++i ) {
		pipePool.push_back(new Pipe(pipesz));
	}
}

void PlanNode::ClearPipePool() {
	for (auto &pipe_ptr : pipePool) {		
		delete pipe_ptr;
	}
	pipePool.clear();
}
