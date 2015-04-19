#include "PlanTree.h"

void print_Operand(struct Operand *pOperand) {
	if(pOperand!=NULL){
		cout<< pOperand->value;
	} else
		return;
}

void print_ComparisonOp(struct ComparisonOp *pCom){
	if(pCom!=NULL)        {
		print_Operand(pCom->left);
		switch(pCom->code) {
            case 5:
                    cout << " < "; break;
            case 6:
                    cout << " > "; break;
            case 7:
                    cout << " = ";
        }
        print_Operand(pCom->right);
    } else {
        return;
    }
}

void print_OrList(struct OrList *pOr) {
    if(pOr !=NULL) {
        struct ComparisonOp *pCom = pOr->left;
        print_ComparisonOp(pCom);
        if(pOr->rightOr) {
            cout << " OR ";
            print_OrList(pOr->rightOr);
        }
    } else {
        return;
    }
}

void print_AndList(struct AndList *pAnd) {
    if(pAnd !=NULL) {
        struct OrList *pOr = pAnd->left;
        cout << "(";
        print_OrList(pOr);
        cout << ") ";
        if(pAnd->rightAnd) {
            cout << "AND ";
            print_AndList(pAnd->rightAnd);
        }
    } else {
        return;
    }
}


string tbl_prefix = "/cise/homes/rui/Desktop/testfiles/10M_table/";
string dbf_prefix = "dbfile/";
string tbl_name[8] = {"region", "nation", "customer", "part", "partsupp", "supplier", "order", "lineitem"};
enum Table { REGION = 0, NATION, CUSTOMER, PART, PARTSUPP, SUPPLIER, ORDER, LINEITEM };
string catalog_path = "catalog";

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

// remove the prefix relation name for growing parse tree
void PlanTree::RemovePrefix(struct Operand *pOp) {	
	string fullname(pOp->value);
	int pos = fullname.find(".");	
	string name = fullname.substr(pos+1);
	strcpy(pOp->value, (char*)name.c_str());		
}

void PlanTree::SeparatePredicate(struct AndList *pAnd) {
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
				RemovePrefix(pCom->left);
				std::string lname(pCom->left->value);				
				RemovePrefix(pCom->right);
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
				std::string name;
				if (NAME == lOperand) {
					RemovePrefix(pCom->left);
					name = std::string(pCom->left->value);	
				} else if(NAME == rOperand){
					RemovePrefix(pCom->right);
					name = std::string(pCom->right->value);	
				}
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
	std::vector<int> joinedTable(numToJoin, 0);
	std::vector<PlanNode*> nodeList;
	std::vector<PlanNode*> joinNodes;
	nodeList.reserve(numToJoin);
	SeparatePredicate(pAnd);
	
	GrowSelectFileNode(nodeList);
	s.Print();
	if (numToJoin < 2) {
		return;
	}
	int numOfRels = 2;
	//use greedy algorithm, find the minimum cost join and apply
	double min = std::numeric_limits<double>::max();
	std::vector<char*> minList;
	auto minit = joinList.begin();
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
	treeNode->lchild = nodeList[minit->left];
	treeNode->rchild = nodeList[minit->right];
	nodeList[minit->left]->parent = treeNode;
	nodeList[minit->right]->parent = treeNode;	
	joinNodes.push_back(treeNode);

	s.Apply(minit->p, &minList[0] , 2);
	joinList.erase(minit);
	// find if any cross selection can be performed
	GrowSelectPipeNode(joinedTable, minList, joinNodes, numOfRels);
	s.Print();	

	++numOfRels;
	// keep finding minimum cost join and apply while joinList is not empty 
	while(!joinList.empty()) {		
		int product_cout = 0;
		min = std::numeric_limits<double>::max();
		int minId = -1, newId = -1;
		std::vector<char*> rels = minList;
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
		joinedTable[minId] = 1;
		minList.push_back(relNames[minId]);
		Record *rec = new Record();
		CNF *cnf = new CNF();	
		TableInfo &rti = tableInfo.at(tableList.at(relNames[minId]));
		cnf->GrowFromParseTree (minit->p, joinNodes[numOfRels - 3]->outSch, rti.sch, *rec);
		JoinNode *treeNode = new JoinNode(cnf, rec);
		treeNode->outSch = new Schema(*(joinNodes[numOfRels - 3]->outSch), *rti.sch);

		treeNode->lchild = joinNodes[numOfRels - 3];
		treeNode->rchild = nodeList[minId];
		joinNodes[numOfRels - 3]->parent = treeNode;
		nodeList[minId]->parent = treeNode;
		joinNodes.push_back(treeNode);
		s.Apply(minit->p, &minList[0] , numOfRels);
		joinList.erase(minit);
		// find if any cross selection can be performed
		GrowSelectPipeNode(joinedTable, minList, joinNodes, numOfRels);
		int predNo;
		while ( (predNo = CheckCrossSelect(crossSelectList, joinedTable)) != -1) {
			s.Apply(crossSelectList[predNo].p, &minList[0] , numOfRels);
			Record *rec = new Record();
			CNF *cnf = new CNF();	
			cnf->GrowFromParseTree (crossSelectList[predNo].p, joinNodes[numOfRels - 2]->outSch, *rec);
			PlanNode *treeNode = new SelectPipeNode(cnf, rec);
			crossSelectList.erase(crossSelectList.begin() + predNo);
		}
		s.Print();
		// increment number of relations
		++numOfRels;
	}
}

void PlanTree::GrowSelectFileNode(std::vector<PlanNode*> &nodeList) {
	for (int i = 0; i < numToJoin; ++i) {
		TableInfo &ti = tableInfo.at(tableList.at(relNames[i]));
		Record *rec = new Record();
		CNF *cnf = new CNF();
		PlanNode *treeNode = nullptr;
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
		nodeList.push_back(treeNode);
	}
}

void PlanTree::GrowSelectPipeNode(std::vector<int> &joinedTable, std::vector<char*> &minList, std::vector<PlanNode*> &joinNodes, int numOfRels) {
	int predNo;
	while ( (predNo = CheckCrossSelect(crossSelectList, joinedTable)) != -1) {
		s.Apply(crossSelectList[predNo].p, &minList[0] , numOfRels);
		Record *rec = new Record();
		CNF *cnf = new CNF();	
		cnf->GrowFromParseTree (crossSelectList[predNo].p, joinNodes[numOfRels - 2]->outSch, *rec);
		PlanNode *treeNode = new SelectPipeNode(cnf, rec);
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