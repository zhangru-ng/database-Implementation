#include "Statistics.h"

Statistics::Statistics () {
	relations.reserve(8);
}

Statistics::Statistics (Statistics const &copyMe) {
	relations = copyMe.relations;
}

void Statistics::AddRel (char *relName, long numTuples) {
	// if the relation name equal to NULL
	if (nullptr == relName) {
		cerr << "ERROR: Invalid relation name in AddRel\n";
		exit(1);
	}
	// add this relation in
	auto p = relations.emplace(relName, RelInfo(numTuples));
	// if the relation is in statisics, upadte number of tuples of this relation
	if (false == p.second) {
		//p.first = Relations::iterator point to the matched element
		(p.first)->second.numTuples = numTuples;		
	} 
}

void Statistics::AddAtt (char *relName, char *attName, long numDistincts) {
	//if the names are invalid, report and exit
	if (nullptr == relName || nullptr == attName) {
		cerr << "ERROR: Invalid relation name or attribute name in AddAtt\n";
		exit(1);
	}
	// if the relation is not in statisics, report error and exit
	if (relations.find(relName) == relations.end()) {
		cerr << "ERROR: Attempt to add attribute to non-exist relation!\n";
		exit(1);
	} 
	// obtain attribute list of this relation
	auto &atts = relations.at(relName).atts;
	auto p = atts.emplace(attName, numDistincts);
	// if the attribute is in statisics, upadte number of distinct value of this attribute
	if (false == p.second) {
		//p.first = AttInfo::iterator point to the matched element
		(p.first)->second = numDistincts;		
	}
}

void Statistics::CopyRel (char *oldName, char *newName) {
	// if the old relation is not in statisics, report error and exit
	auto iter = relations.find(oldName);
	if (relations.end() == iter) {
		cerr << "ERROR: Attempt to copy non-exist relation!\n";
		exit(1);
	}
	RelInfo &old = iter->second;
	RelInfo rel(old.numTuples);
	for (auto &att : old.atts) {
		std::string newAttName(newName);
		newAttName += "." + att.first;
		rel.atts.emplace(newAttName, att.second);
	}
	// //Add into relations with new name
	relations.emplace(newName, rel);
}
	
void Statistics::Read (const char *fromWhere) {
	std::ifstream infile(fromWhere);
	if (!infile.is_open()) {
		return;
	}
	std::string relName;
	std::string attName;
	long numTuples, numDistincts;
	AttInfo atts;
	atts.reserve(16);
	while (infile.good()) {
		infile >> relName;
		infile >> numTuples;
		RelInfo info(numTuples);	
		while (1) {
			infile >> attName;
			if ("REL_END" == attName) {
				break;
			}
			infile >> numDistincts;
			info.atts.emplace(attName, numDistincts);
		}
		relations.emplace(relName, std::move(info));
		atts.clear();
	}
}

void Statistics::Write (const char *fromWhere) {
	std::ofstream outfile(fromWhere);
	if (!outfile.is_open()) {
		cerr << "ERROR: Can not open output file in Statistics::Write\n";
		exit(1);
	}
	for (auto const &rel : relations) {
		outfile << rel.first << " " << rel.second.numTuples << endl;
		for (auto att : rel.second.atts) {
			outfile << att.first << " " << att.second << endl;
		}
		outfile << "REL_END" <<endl;
		outfile << endl;
	}
}

void Statistics::Apply (const struct AndList *parseTree, char *relNames[], int numToJoin) {
	auto nsp = Simulate(parseTree, relNames, numToJoin);
	double totNumTuples = std::get<0>(nsp);
	float selectivity = std::get<1>(nsp);
	cout << "Apply:" << totNumTuples * selectivity << endl;
	std::shared_ptr<Subset> sp = std::make_shared<Subset>(totNumTuples * selectivity);
	for (int i = 0; i < numToJoin; i++) {
		RelInfo &rel = relations.at(relNames[i]);
		rel.hasJoined = true;
		rel.sp = sp;
		AttInfo &atts = rel.atts;
		for (auto const &att : atts) {
			sp->atts.emplace(att.first, att.second * selectivity);
		}
		sp->names.push_back(relNames[i]);
	}

	// for (int i = 0; i < numToJoin; i++) {
	// 	cout << "N"<< relations.at(relNames[i]).sp->numTuples << endl;
	// }
}

double Statistics::Estimate (const struct AndList *parseTree, char **relNames, int numToJoin) {
	//number of tuples and selectivity pair
	auto nsp = Simulate(parseTree, relNames, numToJoin);
	double totNumTuples = std::get<0>(nsp);
	float selectivity = std::get<1>(nsp);
	cout << "Estimate" << totNumTuples * selectivity << endl;
	return totNumTuples * selectivity;

}

std::pair<double,float> Statistics::Simulate (const struct AndList *parseTree, char **relNames, int numToJoin) {
	std::unordered_set<std::string> relToJoin;
	for (int i = 0; i < numToJoin; i++) {
		//if the relations to join is not in current statistics, report error and exit
		if (relations.find(relNames[i]) == relations.end()) {
			cerr << "ERROR: Current statistics does not contain " << relNames << endl;
			exit(1);
		}
		//otherwis hash them into a hash table relToJoin
		relToJoin.emplace(relNames[i]);
	}
	int numSet = 0;
	std::vector<std::string> rn;
	//while there is elements in the hash table
	while (relToJoin.size() > 0) {
		auto rel = relToJoin.begin();
		auto iter = relations.find(*rel);
		rn.push_back(iter->first);
		//get the subset list of first element in the table
		if (false == iter->second.hasJoined) {
			//erase this element in the hash table after get its subset member
			relToJoin.erase(rel);
		} else {
			std::vector<std::string> &names = iter->second.sp->names;		
			//for each elements in the subset 
			for (auto const &srel : names) {
				//if one of the elements in the subset is not in relations to join, this join does not make sense
				//because all relation in the subset list have been joined into one relation
				if (relToJoin.find(srel) == relToJoin.end()) {
					cerr << "ERROR: Attempt to join incomplete subset members\n";
					exit(1);
				} else {
					//erase the matched relation
					relToJoin.erase(srel);
				}
			}
		}		
		++numSet;
	}
	if (1 == numSet) {
		if (nullptr != parseTree) {
			return EstimateSelection(parseTree, rn);
		}		
	} else if (2 == numSet) {
		if (nullptr == parseTree) {
			return EstimateProduct(rn);
		} else {
			return EstimateJoin(parseTree, rn);
		}
	} else {
		cerr << "ERRPR: Attempt to perform operation on more than 2 relations!\n";
		exit(1);
	}
}
// "(l_returnflag = 'R') AND (l_discount < 0.04 OR l_shipmode = 'MAIL')";

std::pair<double,float> Statistics::EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname) {		
	struct OrList *pOr = pAnd->left;
	struct ComparisonOp *pCom = pOr->left;
	std::string name = initAttsName(pCom);
	RelInfo &rel = relations.at(relname[0]);
	AttInfo &atts = rel.hasJoined == false? rel.atts : rel.sp->atts;
	float totSel = 1.0f;
	std::vector<std::string> attNames; 
	while (pAnd != nullptr) {
		pOr = pAnd->left;
		float orSel = 1.0f;		
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			std::string name = initAttsName(pCom);
			auto iter = atts.find(name);
			if(atts.end() == iter) {
				cerr << "ERROR: Attempt to select on non-exist attribute!\n";
				exit(1);
			}
			int op = pCom->code;			
			float sel =  EQUALS == op ? 1.0f / atts[name] : 1.0f / 3;
			// if this attribute is not met before, it is independent with other attibutes
			if (attNames.empty()) {
				orSel = sel;
				attNames.push_back(name);
			} else if (std::find(attNames.begin(), attNames.end(), name) == attNames.end()) {
				orSel = 1 - (1 - orSel) * (1 -sel);
				attNames.push_back(name);
			} else {
				orSel += sel;
			}
			pOr = pOr->rightOr;
		}
		orSel = orSel < 1 ? orSel : 1;
		totSel *= orSel;
		attNames.clear();
		pAnd = pAnd->rightAnd;
	}	
	return std::make_pair(rel.numTuples, totSel);
}

std::string Statistics::initAttsName (struct ComparisonOp *pCom) {
	int op = pCom->code;
	// NAME , value | value , name
	if (pCom->left->code == NAME) {
		return std::string(pCom->left->value);				
	} else {
		return std::string(pCom->right->value);
	}
}

std::pair<double,float> Statistics::EstimateJoin(const struct AndList *pAnd, std::vector<std::string> &relname) {
	RelInfo &lrel = relations.at(relname[0]);
	RelInfo &rrel = relations.at(relname[1]);
	AttInfo &latts = lrel.hasJoined == false? lrel.atts : lrel.sp->atts;
	AttInfo &ratts = rrel.hasJoined == false? rrel.atts : rrel.sp->atts;
	double totTuples;
	float totSel = 1.0f;
	while (pAnd != nullptr) {
		struct OrList *pOr = pAnd->left;
		float orSel = 1.0f;
		std::vector<std::string> attNames;  
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			int op = pCom->code;
			// NAME = NAME
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME = NAME
			if (EQUALS == op && NAME == (lOperand | rOperand)) {
				std::string lname(pCom->left->value);
				std::string rname(pCom->right->value);				
				auto liter = latts.find(lname);
				auto riter = ratts.find(rname);
				if (liter == latts.end() || riter == ratts.end()) {
					liter = ratts.find(lname);
					riter = latts.find(rname);
					if (liter == ratts.end() || riter == latts.end()) {
						cerr << "ERROR: Attempt to join on non-exist attribute!\n" ;
						exit(1);
					}
				}
				double lTuples = lrel.hasJoined == false? lrel.numTuples : lrel.sp->numTuples;
				double rTuples = rrel.hasJoined == false? rrel.numTuples : rrel.sp->numTuples;
				long max = std::max(liter->second, riter->second);
				totTuples = (lTuples * rTuples) / max;
			}
			// NAME , value | value , name
			else if (lOperand | rOperand > 4) {
				std::string name = initAttsName(pCom);
				auto iter = latts.find(name);
				if (iter == latts.end()) {
					iter = ratts.find(name);
					if (iter == ratts.end()) {
						cerr << "ERROR: Attempt to select on non-exist attribute!\n";
						exit(1);
					}
				}
				float sel =  EQUALS == op ? 1.0f / iter->second : 1.0f / 3;
				if (attNames.empty()) {
					orSel = sel;
					attNames.push_back(name);
				} else if (std::find(attNames.begin(), attNames.end(), name) == attNames.end()) {
					orSel = 1 - (1 - orSel) * (1 -sel);
					attNames.push_back(name);
				} else {
					orSel += sel;
				}
			} else {
				cout << "Not yet\n";
				exit(1);
			}
			pOr = pOr->rightOr;
		}
		orSel = orSel < 1 ? orSel : 1;
		totSel *= orSel;
		pAnd = pAnd->rightAnd;
	}
	return std::make_pair(totTuples, totSel);
}

std::pair<double,float> Statistics::EstimateProduct(std::vector<std::string> &relname) {
	return std::make_pair(relations.at(relname[0]).numTuples * relations.at(relname[1]).numTuples, 1);
}

void Statistics::Print () const {
	for (auto const &rel : relations) {
		cout << "Relation name: " << rel.first << " | num of tuples: " << rel.second.numTuples << endl;
		cout << "Attribute list: " << endl;
		for (auto const &att : rel.second.atts) {
			cout << "Attribute name: " << att.first << " | num of distinct: " << att.second << endl;
		}
		cout << endl;
	}
}
