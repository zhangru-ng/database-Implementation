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
		cerr << "ERROR: Invalid relation name in AddRel!\n";
		exit(1);
	}
	// add this relation in
	auto p = relations.emplace(std::string(relName), std::make_shared<RelInfo>(numTuples));
	// if the relation is in statisics, upadte number of tuples of this relation
	if (false == p.second) {
		auto relInfo_ptr = (p.first)->second;
		//p.first = Relations::iterator point to the matched element
		relInfo_ptr->numTuples = numTuples;
	} 
}

void Statistics::AddAtt (char *relName, char *attName, long numDistincts) {
	//if the names are invalid, report and exit
	if (nullptr == relName || nullptr == attName) {
		cerr << "ERROR: Invalid relation name or attribute name in AddAtt!\n";
		exit(1);
	}
	// if the relation is not in statisics, report error and exit
	if (relations.find(relName) == relations.end()) {
		cerr << "ERROR: Attempt to add attribute to non-exist relation!\n";
		exit(1);
	} 
	if (relations.at(relName)->numTuples < numDistincts) {
		cerr << "ERROR: Attempt to add distinct value greater than number of tuples!\n";
		exit(1);
	}
	relations.at(relName)->AddAtt(attName, numDistincts);
}

void Statistics::CopyRel (char *oldName, char *newName) {
	// if the old relation is not in statisics, report error and exit
	auto iter = relations.find(oldName);
	if (relations.end() == iter) {
		cerr << "ERROR: Attempt to copy non-exist relation! " << oldName << "\n";
		exit(1);
	}
	// get number of tuples in old relaiton
	RelInfo &old = *(iter->second);
	if (old.hasJoined) {
		cerr << "ERROR: Can't copy relations which have been joined!\n";
	}
	// check if the new name is already exist in the statistics
	iter = relations.find(newName);
	if (relations.end() != iter) {
		cerr << "ERROR: Relation names conflict in CopyRel!\n";
		exit(1);
	}	
	RelInfo rel(old.numTuples);
	rel.hasJoined = old.hasJoined;
	//rename the attribute and add to attribute list
	for (auto &att : old.atts) {
		// std::string newAttName(newName);
		// newAttName += "." + att.first;
		// rel.AddAtt(newAttName, att.second);
		rel.AddAtt(att.first, att.second);
	}
	relations.emplace(std::move(newName), std::make_shared<RelInfo>(std::move(rel)));
}
	
 void Statistics::Read (const char *fromWhere) {
	std::ifstream infile(fromWhere);
	if (!infile.is_open()) {
		return;
	}
	std::string relNames;
	std::string attName;
	double numTuples, numDistincts;
	relations.clear();
	while (true) {
		infile >> relNames;
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
		// read the attribute list
		while (true) {
			infile >> attName;
			if(attName == "}" || !infile.good()) {
				break;
			}
			infile >> numDistincts;
			info.atts.emplace(attName, numDistincts);
		}
		if (info.hasJoined) {
			// read the subset list
			infile >> attName;
			if ("[" != attName) {
				cerr << "ERROR: Malformed subset list in Read\n";
				exit(1);
			}
			Subset subset;
			while (true) {
				infile >> attName;	
				if (attName == "]" || !infile.good()) {
					break;
				}		
				subset.push_back(attName);
			} 			
			auto relInfo_ptr = std::make_shared<RelInfo>(std::move(info));
			// all relation in the subset point to the same relaition infomation
			for (auto  &s : subset) {
				relations.insert(std::make_pair(s, relInfo_ptr));
			}
			relInfo_ptr->subset = std::move(subset);			
		} else {
			relations.emplace(relNames, std::make_shared<RelInfo>(std::move(info)));
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
			auto relInfo_ptr = rel.second;
			outfile << rel.first << " " << relInfo_ptr->numTuples << endl;
			outfile << relInfo_ptr->hasJoined << endl;
			outfile << "{" <<  endl;
			// write out the attribute list, surround with curly braces
			for (auto const &att : relInfo_ptr->atts) {
				outfile << att.first << " " << att.second << endl;
			}
			outfile << "}" <<endl;
			if (relInfo_ptr->hasJoined) {
				outfile << "[" << endl;
				// write out the subset list, surround with square braces
				for (auto const &n : relInfo_ptr->subset) {
					outfile << n << " ";
					names.insert(n);
				}
				outfile << endl;
				outfile << "]" <<endl;
			}
		}		
	}
}

void Statistics::Apply (const struct AndList *parseTree, char **relNames, int numToJoin) {
	std::vector<std::string> repOfSet;
	auto estimateInfo = Simulate(parseTree, relNames, numToJoin, repOfSet);
	double totNumTuples = std::get<0>(estimateInfo);
	float selectivity = std::get<1>(estimateInfo);
	// cout << "Apply:" << totNumTuples * selectivity << endl;	
	RelInfo &rel1 = *(relations.at(repOfSet[0]));
	AttInfo &atts1 = rel1.atts;
	rel1.numTuples = totNumTuples * selectivity;
	for (auto &att1 : atts1) {
		// attInfo->emplace(att.first, UNKNOWN != att.second ? att.second * leftSel : UNKNOWN);
		if(UNKNOWN != att1.second) {
			double numDistincts = att1.second * selectivity;
			// distinct value is at lease 1
			att1.second = (numDistincts < 1) ? 1: numDistincts;
		}
	}
	int numSets = repOfSet.size();
	if (2 == repOfSet.size()) {
		RelInfo &rel2 = *(relations.at(repOfSet[1]));
		AttInfo &atts2 = rel2.atts;
		// Build the new attribute list
		for (auto const &att2 : atts2) {
			double numDistincts = UNKNOWN;
			if (UNKNOWN != att2.second) {
				numDistincts = att2.second * selectivity;
				// distinct value is at lease 1
				numDistincts = (numDistincts < 1) ? 1: numDistincts;
			}
			// atts->emplace(att.first, UNKNOWN != att.second ? att.second * rightSel : UNKNOWN);
			atts1.emplace(att2.first, numDistincts);
		}
		rel1.hasJoined = true;
		// Build the subset list
		// if subset of relation 1 is empty, add itself to the subset
		if(0 == rel1.subset.size()) {
			rel1.subset.push_back(repOfSet[0]);
		}  
		// similarily for relation 2
		if (0 == rel2.subset.size()) {
			rel1.subset.push_back(repOfSet[1]);
			// make relation 2 point to the same RelInfo as relation 1
			relations.at(repOfSet[1]) = relations.at(repOfSet[0]);
		} else {
			// if relation 2 has subset, make all of the subset point to the same RelInfo as relation 1
			for (std::string &name : rel2.subset) {
				rel1.subset.push_back(name);
				relations.at(name) = relations.at(repOfSet[0]);
			}
		}	
	}	
}

double Statistics::Estimate (const struct AndList *parseTree, char **relNames, int numToJoin) {
	std::vector<std::string> repOfSet;
	//number of tuples and selectivity pair
	auto estimateInfo = Simulate(parseTree, relNames, numToJoin, repOfSet);
	double totNumTuples = std::get<0>(estimateInfo);
	float selectivity = std::get<1>(estimateInfo);
	// cout << "Estimate" << totNumTuples * selectivity << endl;
	return totNumTuples * selectivity;

}

EstimateInfo Statistics::Simulate (const struct AndList *parseTree, char **relNames, int numToJoin, std::vector<std::string> &repOfSet) {
	std::unordered_set<std::string> relToJoin;
	for (int i = 0; i < numToJoin; i++) {
		//if the relations to join is not in current statistics, report error and exit
		if (relations.find(relNames[i]) == relations.end()) {
			cerr << "ERROR: Current statistics does not contain " << relNames[i] << endl;
			exit(1);
		}
		//otherwise only hash it into the table
		else {
			relToJoin.emplace(relNames[i]);
		}
		
	}
	int numSet = 0;
	//while there is elements in the hash table
	while (relToJoin.size() > 0) {
		auto rel = relToJoin.begin();
		auto iter = relations.find(*rel);
		repOfSet.push_back(iter->first);
		//get the subset list of first element in the table
		if (false == iter->second->hasJoined) {
			//erase this element in the hash table after get its subset member
			relToJoin.erase(rel);
		} else {
			Subset &names = iter->second->subset;		
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
	// if all "relations" belong to the same subset
	if (1 == numSet) {
		// estimate selection when the parse tree is not empty
		if (nullptr != parseTree) {
			return EstimateSelection(parseTree, repOfSet);
		}		
	} else if (2 == numSet) {
		// estimate cross product when the parse tree is empty
		if (nullptr == parseTree) {
			return EstimateProduct(repOfSet);
		} else {
			return EstimateJoin(parseTree, repOfSet);
		}
	} else {
		// each time only estimate operations on one or two relations
		cerr << "ERRPR: Attempt to perform operation on more than 2 relations!\n";
		exit(1);
	}
}

// estimate cost of selection when all "relations" belong to the same subset
EstimateInfo Statistics::EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname) {		
	struct OrList *pOr = pAnd->left;
	struct ComparisonOp *pCom = pOr->left;
	std::string name = InitAttsName(pCom);
	RelInfo &rel = *(relations.at(relname[0]));
	AttInfo &atts = rel.atts;
	float totSel = 1.0f;
	std::vector<std::string> attNames; 
	while (pAnd != nullptr) {
		pOr = pAnd->left;
		float orSel = 1.0f;		
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			std::string name = InitAttsName(pCom);
			// since all "relations" belong to the same subset, which means only one relation in this estimation
			// check if the attribute in this relation's attribute list
			auto iter = atts.find(name);
			if(atts.end() == iter) {
				cerr << "ERROR: Attempt to select on non-exist attribute! " << name << "\n";
				exit(1);
			}
			int op = pCom->code;
			// equal seletion: selectivity = 1 / V(A)
			// non-equal seletion: selectivity = 1 / 3
			float sel =  (EQUALS == op) ? 1.0f / atts[name] : 1.0f / 3;			
			SetSelectivity(name, attNames, orSel, sel);
			pOr = pOr->rightOr;
		}
		// multiply selectivities from each or list
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

// initial attribute name from parse tree
std::string Statistics::InitAttsName (struct ComparisonOp *pCom) {
	int op = pCom->code;
	// NAME , value | value , NAME
	if (pCom->left->code == NAME) {
		return std::string(pCom->left->value);				
	} else {
		return std::string(pCom->right->value);
	}
}

// estimate cost of joining two relation, maybe along with some selections
EstimateInfo Statistics::EstimateJoin(const struct AndList *pAnd, std::vector<std::string> &relname) {
	RelInfo &lrel = *(relations.at(relname[0]));
	RelInfo &rrel = *(relations.at(relname[1]));
	AttInfo &latts = lrel.atts;
	AttInfo &ratts = rrel.atts;
	double totTuples;
	float totSel = 1.0f;
	while (pAnd != nullptr) {
		struct OrList *pOr = pAnd->left;
		float orSel = 1.0f;
		std::vector<std::string> attNames;
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			int op = pCom->code;
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME op NAME  (NAME = 4, thus NAME | NAME = 4)
			if (NAME == (lOperand | rOperand)) {
				std::string lname(pCom->left->value);
				std::string rname(pCom->right->value);
				// check if each of the two attributes belong to the relations
				auto numDistinctsPair = GetAttsDistinct(lname, rname, latts, ratts);
				double lTuples = lrel.numTuples;
				double rTuples = rrel.numTuples;
				double max = std::max(numDistinctsPair.first, numDistinctsPair.second);				
				if (EQUALS == op) {
					// equal join : number of tuples = (T(R) * T(S)) / max{V(A), V(B)}
					totTuples = (lTuples * rTuples) / max;
				} else {
					// non-equal join : number of tuples = (1/3) * (T(R) * T(S)) 
					totTuples = (lTuples * rTuples) / 3;
				}				
			}
			// NAME op value | value op name (NAME = 4 other = [1, 3], thus NAME | other > 4)
			else if (lOperand | rOperand > 4) {
				std::string name = InitAttsName(pCom);
				//pair store the number of distinct and which side the attribute belong to
				double distinct = GetAttsDistinct(name, latts, ratts);
				float sel =  (EQUALS == op) ? 1.0f / distinct : 1.0f / 3;
				SetSelectivity(name, attNames, orSel, sel);
			} else {
				cout << "ERROR: Both operands are literals\n";
				exit(1);
			}
			pOr = pOr->rightOr;
		}
		totSel *= orSel;
		pAnd = pAnd->rightAnd;
	}
	if (totSel < 0) {
		cerr << "ERROR: Total selectivity less than 0\n";
		exit(1);
	}
	return std::make_pair(totTuples, totSel);
}

EstimateInfo Statistics::EstimateProduct(std::vector<std::string> &relname) {
	return std::make_pair(relations.at(relname[0])->numTuples * relations.at(relname[1])->numTuples, 1);
}

void Statistics::SetSelectivity(std::string &name, std::vector<std::string> &attNames, float &selToSet, float sel) {
	// if the name list is empty, initial the selectivity
	if (attNames.empty()) {
		selToSet = sel;
		attNames.push_back(name);
	} 
	// if this attribute is not met before, it is independent with other attibutes
	else if (std::find(attNames.begin(), attNames.end(), name) == attNames.end()) {
		selToSet = 1 - (1 - selToSet) * (1 -sel);
		attNames.push_back(name);
	} 
	// if this attribute is met before, it is not independent 
	else {
		selToSet += sel;
	}
	// if the selectivity become greater than 1, set it to be 1
	selToSet = selToSet < 1 ? selToSet : 1;
}

double Statistics::GetAttsDistinct(std::string &name, AttInfo &latts, AttInfo &ratts) {
	auto iter = latts.find(name);
	//if the attribute is not in left relation
	if (iter == latts.end()) {
		iter = ratts.find(name);
		//if the attribute is not in right relation
		if (iter == ratts.end()) {
			cerr << "ERROR: Attempt to select on non-exist attribute! " << name << "\n";
			exit(1);
		}
	}
	return iter->second;
}


std::pair<double, double> Statistics::GetAttsDistinct(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts) {
	auto liter = latts.find(lname);
	auto riter = ratts.find(rname);
	// There are only two cases:
	// 1.left attribute name belong to left attribute list, right attribute name belong to right attribute list
	// 2.right attribute name belong to left attribute list, left attribute name belong to right attribute list
	if (liter == latts.end() || riter == ratts.end()) {
		liter = ratts.find(lname);
		riter = latts.find(rname);
		if (liter == ratts.end() || riter == latts.end()) {
			cerr << "ERROR: Attempt to join on non-exist attribute! " << lname << "=" << rname << "\n" ;
			exit(1);
		}
	}
	return std::make_pair(liter->second, riter->second);
}

int Statistics::GetNumTuples (const std::string &relName) const {
	if(relations.find(relName) == relations.end()) {
		cerr << "ERROR: Relation does not appear in Statistics!" << endl;
		exit(1);
	}
	return relations.at(relName)->numTuples;
}

void Statistics::Print () const {
	std::unordered_set<std::string> names;
	for (auto const &rel : relations) {
		// avoid visit relations belong to the same subset twice
		if (names.end() == names.find(rel.first)) {
			names.insert(rel.first);
			cout <<  "Relation name: " << rel.first << " | num of tuples: " << rel.second->numTuples << endl;
			cout << (rel.second->hasJoined ? "Joined" : "Not joined") << endl;
			cout << "Attribute list: " << endl;
			cout << "{" <<  endl;
			// print attribute list
			for (auto const &att : rel.second->atts) {
				cout << "\t" << att.first << " " << att.second << endl;
			}
			cout << "}" <<endl;			
			if (rel.second->hasJoined) {
				// print subset list
				cout << "Subset list: " << endl;
				cout << "[" << endl;
				for (auto const &n : rel.second->subset) {
					cout << n << " ";
					// insert subset member into visited relation set
					names.insert(n);
				}
				cout << endl;
				cout << "]" <<endl;
			}
		}
		cout << endl;
	}	
	cout << endl;
}

int	Statistics::FindAtts(char **relNames, std::string &AttsName, int numToJoin) const {
	for (int i = 0; i < numToJoin; ++i) {
		AttInfo &atts = relations.at(relNames[i])->atts;
		if(atts.find(AttsName) != atts.end()){
			return i;
		}
	}
	cerr << AttsName << endl;
	cerr << "ERROR: attribute is non-exist!";
	exit(1);
}

int Statistics::CheckRels(const char* relName) const {	
	if (relations.find(relName) == relations.end()) {
		cerr << "ERROR: Current statistics does not contain " << relName << endl;
		return DISCARD;
	}
	return RESUME;
}