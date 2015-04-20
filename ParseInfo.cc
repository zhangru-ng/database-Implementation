#include "ParseInfo.h"

void print_Operand(struct Operand *pOperand) {
	if(pOperand!=NULL){
		cout<< pOperand->value;
	}
}

void print_ComparisonOp(struct ComparisonOp *pCom){
	if(pCom!=NULL)        {
		print_Operand(pCom->left);
		switch(pCom->code) {
            case LESS_THAN:
                    cout << " < "; break;
            case GREATER_THAN:
                    cout << " > "; break;
            case EQUALS:
                    cout << " = ";
        }
        print_Operand(pCom->right);
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
    } 
}

void print_predicate(struct AndList *pAnd) {
	cout << "WHERE predicate: " << endl;
	cout << "{" << endl;
	print_AndList(pAnd);
	cout << "\n}\n" << endl;

}

void print_table_list(struct TableList *tables) {
	struct TableList *p = tables;
	cout << "TABLE list:" << endl;
	cout << "{" << endl;
	while (p) {
		if (p->tableName) {
			cout << p->tableName;
		} 
		if (p->aliasAs) {
			cout << " alias AS " << p->aliasAs << endl;
		}
		p = p->next;
	}
	cout << "}\n" << endl;
}

void print_FuncOperand (struct FuncOperand *fOperand) {
	if (fOperand) {
		cout << fOperand->value << ":";
		switch (fOperand->code) {
			case INT:
				cout << "INT";
				break;
			case DOUBLE:
				cout << "DOUBLE";
				break;			
			case STRING:
				cout << "STRING";
				break;
			case NAME:
				cout << "NAME";
				break;
		}
	}
}

void print_FuncOperator (struct FuncOperator *fOperator) {	
	if(fOperator) {
		if(!fOperator->leftOperand) { cout << "("; }			
		print_FuncOperator(fOperator->leftOperator);
		print_FuncOperand(fOperator->leftOperand);
		if(fOperator->code)
			cout << " " << static_cast<char>(fOperator->code) << " ";
		print_FuncOperator(fOperator->right);
		if(!fOperator->leftOperand) { cout << ")"; }			
    } 
}

void print_aggregate(struct FuncOperator *fOperator) {
	cout << "SUM function: " << endl;
	cout << "{" << endl;	
	print_FuncOperator(fOperator);
	cout << "\n}\n" << endl;
}

void print_distinct(int distinctAtts, int distinctFunc) {
	cout << "DISTINCT(non-aggregate): " << (distinctAtts == 0? "false" : "true") << endl;
	cout << "DISTINCT(aggregate): " << (distinctFunc == 0? "false" : "true") << endl;
	cout << endl;
}

void print_name_list(struct NameList *names) {
	struct NameList *p = names;
	cout << "{" << endl;
	while (p) {
		if (p->name) {
			cout << p->name << endl;
		} 		
		p = p->next;
	}
	cout << "}\n" << endl;
}

void print_groupAtts(struct NameList *names) {
	cout << "GROUP attributes:" << endl;
	print_name_list(names);
}	

void print_selectAtts(struct NameList *names) {
	cout << "SELECT attributes:" << endl;
	print_name_list(names);
}	

void RemovePrefix (struct FuncOperator *fOperator) {	
	if(fOperator) {
		RemovePrefix(fOperator->leftOperator);
		RemoveOperandPrefix(fOperator->leftOperand);
		RemovePrefix(fOperator->right);		
    } 
}

void RemoveOperandPrefix (struct FuncOperand *fOperand) {
	if (fOperand) {
		if (NAME == fOperand->code) {
			string fullname(fOperand->value);
			int pos = fullname.find(".");	
			string name = fullname.substr(pos + 1);
			strcpy(fOperand->value, (char*)name.c_str());
		}		
	}		
}

void RemoveOperandPrefix (struct Operand *pOperand) {
	if(pOperand!=NULL){
		if (NAME == pOperand->code) {
			string fullname(pOperand->value);
			int pos = fullname.find(".");
			string name = fullname.substr(pos+1);
			strcpy(pOperand->value, (char*)name.c_str());
		}
	} 
}

void RemovePrefixComparisonOp(struct ComparisonOp *pCom){
	if(pCom!=NULL)        {
		RemoveOperandPrefix(pCom->left);	
        RemoveOperandPrefix(pCom->right);
    } 
}

void RemovePrefixOrList(struct OrList *pOr) {
    if(pOr !=NULL) {
        struct ComparisonOp *pCom = pOr->left;
        RemovePrefixComparisonOp(pCom);
        if(pOr->rightOr) {        
            RemovePrefixOrList(pOr->rightOr);
        }
    } 
}

void RemovePrefix(struct AndList *pAnd) {
    if(pAnd !=NULL) {
        struct OrList *pOr = pAnd->left;
        RemovePrefixOrList(pOr);
        if(pAnd->rightAnd) {
            RemovePrefix(pAnd->rightAnd);
        }
    } 
}

void InitDefaultPredicate(char *attName, Predicate &initPred) {
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

void CheckGroupAndSelect(struct NameList *groupingAtts, struct NameList *attsToSelect) {
	std::unordered_set<string> names;
	struct NameList *p = groupingAtts;
	while (p) {
		if (p->name) {
			names.insert(p->name);
		} 		
		p = p->next;
	}
	p = attsToSelect;
	while (p) {
		if (p->name) {
			auto ibp = names.insert(p->name);			
			// if the select attribute is not in the groupby name list 
			if(ibp.second) {
				cerr << "SQL ERROR: SELECT on attributes dose not exist in GROUP BY" << endl;
				exit(1);
			}
		} 		
		p = p->next;
	}
}

void CheckSumPredicate(struct FuncOperator *finalFunction, struct NameList *groupingAtts, struct NameList *attsToSelect) {
	if (!groupingAtts && attsToSelect && finalFunction) {
		cerr << "SQL ERROR: SELECT on attributes that does not exist after SUM" << endl;
		exit(1);
	} else if (groupingAtts && attsToSelect) {
		CheckGroupAndSelect(groupingAtts, attsToSelect);
	}
}

void CheckDistinctFunc(struct FuncOperator *finalFunction, struct NameList *groupingAtts) {
	std::unordered_set<string> names;
	struct NameList *p = groupingAtts;
	while (p) {
		if (p->name) {
			names.insert(p->name);
		} 		
		p = p->next;
	}
	CheckFuncOperator(finalFunction, names);
}


void CheckFuncOperand (struct FuncOperand *fOperand, std::unordered_set<string> &names) {
	if (fOperand) {
		auto ibp = names.insert(fOperand->value);
		if(ibp.second) {
			cerr << "SQL ERROR: SUM DISTINCT attributes dose not exist in GROUP BY" << endl;
			exit(1);
		}
	}
}

void CheckFuncOperator (struct FuncOperator *fOperator, std::unordered_set<string> &names) {	
	if(fOperator) {
		CheckFuncOperator(fOperator->leftOperator, names);
		CheckFuncOperand(fOperator->leftOperand, names);
		CheckFuncOperator(fOperator->right, names);
	}
}