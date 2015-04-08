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
	auto p = relations.emplace(std::string(relName), RelInfo(numTuples));
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
	relations.at(relName).AddAtt(attName, numDistincts);
}

void Statistics::CopyRel (char *oldName, char *newName) {
	// if the old relation is not in statisics, report error and exit
	auto iter = relations.find(oldName);
	if (relations.end() == iter) {
		cerr << "ERROR: Attempt to copy non-exist relation!\n";
		exit(1);
	}
	// get number of tuples in old relaiton
	RelInfo &old = iter->second;
	// check if the new name is already exist in the statistics
	iter = relations.find(newName);
	if (relations.end() != iter) {
		cerr << "ERROR: Relation names conflict in CopyRel!\n";
		exit(1);
	}	
	RelInfo rel(old.numTuples);
	for (auto &att : *(old.atts)) {
		std::string newAttName(newName);
		newAttName += "." + att.first;
		rel.AddAtt(newAttName, att.second);
	}
	// //Add into relations with new name
	relations.emplace(std::move(newName), std::move(rel));
}
	
 void Statistics::Read (const char *fromWhere) {
	std::ifstream infile(fromWhere);
	if (!infile.is_open()) {
		return;
	}
	std::string relName;
	std::string attName;
	double numTuples, numDistincts;
	relations.clear();
	while (true) {
		std::shared_ptr<AttInfo> attInfo = std::make_shared<AttInfo>();
		std::shared_ptr<Subset> subSet = std::make_shared<Subset>();
		infile >> relName;
		if (!infile.good()) {
			break;
		}
		infile >> numTuples;
		RelInfo info(numTuples);
		infile >> info.hasJoined;
		infile >> attName;
		if ("{" != attName) {
			cerr << "ERROR: Malformed attribute list in Read\n";
			exit(1);
		}
		while (true) {
			infile >> attName;
			if(attName == "}" || !infile.good()) {
				break;
			}
			infile >> numDistincts;
			attInfo->emplace(attName, numDistincts);
		}
		info.atts = attInfo;		
		if(info.hasJoined) {
			infile >> attName;
			if ("[" != attName) {
				cerr << "ERROR: Malformed subset list in Read\n";
				exit(1);
			}
			while (true) {
				infile >> attName;	
				if (attName == "]" || !infile.good()) {
					break;
				}		
				subSet->push_back(attName);
			} 
			info.subset = subSet;
			for (auto s : *subSet) {
				relations.emplace(s, info);
			}
			
		} else {
			relations.emplace(relName, info);
		}		
	}	
}

void Statistics::Write (const char *fromWhere) {
	std::ofstream outfile(fromWhere);
	std::unordered_set<std::string> names;
	if (!outfile.is_open()) {
		cerr << "ERROR: Can not open output file in Statistics::Write\n";
		exit(1);
	}
	for (auto const &rel : relations) {
		if (names.end() == names.find(rel.first)) {
			names.insert(rel.first);
			outfile << rel.first << " " << rel.second.numTuples << endl;
			outfile << rel.second.hasJoined << endl;
			outfile << "{" <<  endl;
			for (auto att : *(rel.second.atts)) {
				outfile << att.first << " " << att.second << endl;
			}
			outfile << "}" <<endl;
			if (rel.second.hasJoined) {
				outfile << "[" << endl;
				for (auto n : *(rel.second.subset)) {
					outfile << n << " ";
					names.insert(n);
				}
				outfile << endl;
				outfile << "]" <<endl;
			}
		}		
	}
	// outfile << "END_STAT";
}

void Statistics::Apply (const struct AndList *parseTree, char *relNames[], int numToJoin) {
	auto nsp = Simulate(parseTree, relNames, numToJoin);
	double totNumTuples = std::get<0>(nsp);
	float selectivity = std::get<1>(nsp);
	cout << "Apply:" << totNumTuples * selectivity << endl;
	std::shared_ptr<AttInfo> attInfo = std::make_shared<AttInfo>();
	std::shared_ptr<Subset> subSet = std::make_shared<Subset>();
	// Build the new attribute list and subset list
	for (int i = 0; i < numToJoin; i++) {
		RelInfo &rel = relations.at(relNames[i]);
		AttInfo &atts = *(rel.atts);
		for (auto const &att : atts) {
			attInfo->emplace(att.first, UNKNOWN != att.second ? att.second * selectivity : UNKNOWN);
		}
		subSet->push_back(relNames[i]);
	}
	for (int i = 0; i < numToJoin; i++) {
		RelInfo &rel = relations.at(relNames[i]);		
		rel.numTuples = totNumTuples * selectivity;
		rel.hasJoined = true;
		rel.atts = attInfo;
		rel.subset = subSet;
	}		
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
			cerr << "ERROR: Current statistics does not contain " << relNames[i] << endl;
			exit(1);
		}
		//otherwis hash them into a hash table relToJoin
		if (relations.at(relNames[i]).hasJoined) {
			Subset &names = *(relations.at(relNames[i]).subset);
			for (auto const &srel : names) {
				relToJoin.emplace(srel);
			}
		} else {
			relToJoin.emplace(relNames[i]);
		}
		
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
			Subset &names = *(iter->second.subset);		
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
	AttInfo &atts = *(rel.atts);
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
	if (totSel < 0) {
		cerr << "ERROR: Total selectivity less than 0\n";
		exit(1);
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
	AttInfo &latts = *(lrel.atts);
	AttInfo &ratts = *(rrel.atts);
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
				auto numDistinctsPair = CheckAtts(lname, rname, latts, ratts);
				double lTuples = lrel.numTuples;
				double rTuples = rrel.numTuples;
				double max = std::max(numDistinctsPair.first, numDistinctsPair.second);
				totTuples = (lTuples * rTuples) / max;
			}
			// NAME , value | value , name
			else if (lOperand | rOperand > 4) {
				std::string name = initAttsName(pCom);
				long numDistincts = CheckAtts(name, latts, ratts);
				float sel =  EQUALS == op ? 1.0f / numDistincts : 1.0f / 3;
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
	if (totSel < 0) {
		cerr << "ERROR: Total selectivity less than 0\n";
		exit(1);
	}
	return std::make_pair(totTuples, totSel);
}

double Statistics::CheckAtts(std::string &name, AttInfo &latts, AttInfo &ratts) {
	auto iter = latts.find(name);
	if (iter == latts.end()) {
		iter = ratts.find(name);
		if (iter == ratts.end()) {
			cerr << "ERROR: Attempt to select on non-exist attribute!\n";
			exit(1);
		}
	}
	return iter->second;
}


std::pair<double, double> Statistics::CheckAtts(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts) {
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
	return std::make_pair(liter->second, riter->second);
}

std::pair<double,float> Statistics::EstimateProduct(std::vector<std::string> &relname) {
	return std::make_pair(relations.at(relname[0]).numTuples * relations.at(relname[1]).numTuples, 1);
}

void Statistics::Print () const {
	std::unordered_set<std::string> names;
	for (auto const &rel : relations) {
		if (names.end() == names.find(rel.first)) {
			names.insert(rel.first);
			cout <<  "Relation name: " << rel.first << " | num of tuples: " << rel.second.numTuples << endl;
			cout << (rel.second.hasJoined ? "Joined" : "Not joined") << endl;
			cout << "Attribute list: " << endl;
			cout << "{" <<  endl;
			for (auto att : *(rel.second.atts)) {
				cout << att.first << " " << att.second << endl;
			}
			cout << "}" <<endl;
			cout << "Subset list: " << endl;
			if (rel.second.hasJoined) {
				cout << "[" << endl;
				for (auto n : *(rel.second.subset)) {
					cout << n << " ";
					names.insert(n);
				}
				cout << endl;
				cout << "]" <<endl;
			}
		}		
	}	
}
