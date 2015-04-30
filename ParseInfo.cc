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

int CheckGroupAndSelect(struct NameList *groupingAtts, struct NameList *attsToSelect) {
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
				return DISCARD;
			}
		} 		
		p = p->next;
	}
	return RESUME;
}

int CheckSumPredicate(struct FuncOperator *finalFunction, struct NameList *groupingAtts, struct NameList *attsToSelect) {
	if (!groupingAtts && attsToSelect && finalFunction) {
		cerr << "SQL ERROR: SELECT on attributes that does not exist after SUM" << endl;
		return DISCARD;
	} else if (groupingAtts && attsToSelect) {
		return CheckGroupAndSelect(groupingAtts, attsToSelect);
	}
}

struct NameList* GetDistinctName(struct FuncOperator *finalFunction, struct NameList *groupingAtts) {
	std::unordered_set<string> names;
	struct NameList *group = groupingAtts;
	while (group) {
		if (group->name) {
			names.insert(group->name);
		} 		
		group = group->next;
	}
	GetFuncOperator(finalFunction, names);		
	struct NameList *name = (struct NameList *) malloc (sizeof(struct NameList));
	struct NameList *p = name;
	for(auto it = names.begin(); it != names.end(); ) {	
		p->name = strdup(it->c_str());
		if(++it == names.end()) {
			p->next = NULL;
		} else {
			p->next = (struct NameList *) malloc (sizeof(struct NameList));
			p = p->next;
		}		
	}
	return name;
}

void GetFuncOperand (struct FuncOperand *fOperand, std::unordered_set<string> &names) {
	if (fOperand && fOperand->code == NAME) {
		names.insert(fOperand->value);
	}
}

void GetFuncOperator (struct FuncOperator *fOperator, std::unordered_set<string> &names) {	
	if(fOperator) {
		GetFuncOperator(fOperator->leftOperator, names);
		GetFuncOperand(fOperator->leftOperand, names);
		GetFuncOperator(fOperator->right, names);
	}
}

struct NameList* GetSumAtts(struct FuncOperator *fOperator) {
	std::unordered_set<string> sumAtts;
	GetFuncOperator(fOperator, sumAtts);	
	if(sumAtts.empty()) {
		return NULL;
	}
	struct NameList *name = (struct NameList *) malloc (sizeof(struct NameList));
	struct NameList *p = name;
	for(auto it = sumAtts.begin(); it != sumAtts.end(); ) {	
		p->name = strdup(it->c_str());
		if(++it == sumAtts.end()) {
			p->next = NULL;
		} else {
			p->next = (struct NameList *) malloc (sizeof(struct NameList));
			p = p->next;
		}		
	}
	return name;
}

void DestroyNameList(struct NameList *names) {
	struct NameList *p = names;
	while(p) {
		free(p->name);
		struct NameList *temp = p->next;
		free(p);
		p = temp;
	}
}

struct NameList* BuildNameList(std::string name) {
	struct NameList *p = (struct NameList *) malloc (sizeof(struct NameList));
	p->name = strdup(name.c_str());
	p->next = NULL;
	return p;
}

int CountGroupAndSelect(struct NameList *groupingAtts, struct NameList *attsToSelect) {
	std::unordered_set<string> names;
	struct NameList *p = attsToSelect;
	while (p) {
		if (p->name) {
			names.insert(p->name);
		} 		
		p = p->next;
	}
	p = groupingAtts;
	while (p) {
		if (p->name) {
			auto ibp = names.insert(p->name);			
			// if not all group by attribute in select list
			if(ibp.second) {
				return 1;
			}
		} 		
		p = p->next;
	}
	return 0;
}

void BuildGroupSelect(struct NameList *attsToSelect) {
	struct NameList *sum = BuildNameList("SUM");
	struct NameList *p = attsToSelect;
	struct NameList *y;
	while(p) {
		y = p;
		p = p->next;
	}
	y->next = sum;
}
	