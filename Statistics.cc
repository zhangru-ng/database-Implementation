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
		RelInfo &rel = (p.first)->second;
		//p.first = Relations::iterator point to the matched element
		if (true == rel.hasJoined) {
			for (std::string &name : *(rel.subset)) {
				relations.at(name).numTuples = numTuples;
			}
		} else {
			rel.numTuples = numTuples;
		}		
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
		cerr << "ERROR: Attempt to copy non-exist relation! " << oldName << "\n";
		exit(1);
	}
	// get number of tuples in old relaiton
	RelInfo &old = iter->second;
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
	for (auto &att : *(old.atts)) {
		std::string newAttName(newName);
		newAttName += "." + att.first;
		rel.AddAtt(newAttName, att.second);
	}
	//if the relation has joined, copy the subset list
	// if (rel.hasJoined) {
	// 	rel.subset = std::make_shared<Subset>();
	// 	for (std::string const &name : *(old.subset)) {
	// 		if(0 == name.compare(oldName)) {
	// 			rel.subset->push_back(newName);
	// 		} else {
	// 			rel.subset->push_back(name);
	// 		}			
	// 	}
	// }	
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
		if (info.hasJoined) {
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
	std::vector<std::string> repOfSet;
	auto nsp = Simulate(parseTree, relNames, numToJoin, repOfSet);
	double totNumTuples = std::get<0>(nsp);
	float selectivity = std::get<1>(nsp);
	// cout << "Apply:" << totNumTuples * selectivity << endl;	
	RelInfo &rel1 = relations.at(repOfSet[0]);
	AttInfo &atts1 = *(rel1.atts);
	rel1.numTuples = totNumTuples * selectivity;
	for (auto &att : atts1) {
		// attInfo->emplace(att.first, UNKNOWN != att.second ? att.second * leftSel : UNKNOWN);
		if(UNKNOWN != att.second) {
			double numDistincts = att.second * selectivity;
			att.second = (numDistincts < 1) ? 1: numDistincts;
		}
	}
	int numSets = repOfSet.size();
	if (2 == repOfSet.size()) {
		RelInfo &rel2 = relations.at(repOfSet[1]);
		AttInfo &atts2 = *(rel2.atts);
		// Build the new attribute list
		for (auto const &att : atts2) {
			double numDistincts = UNKNOWN;
			if (UNKNOWN != att.second) {
				numDistincts = att.second * selectivity;
				numDistincts = (numDistincts < 1) ? 1: numDistincts;
			}
			// atts->emplace(att.first, UNKNOWN != att.second ? att.second * rightSel : UNKNOWN);
			atts1.emplace(att.first, numDistincts);
		}
		// Build the subset list	
		BuildSubset(rel1, rel2, repOfSet);		
		rel2.atts = rel1.atts;
		rel2.numTuples = rel1.numTuples;
		rel1.hasJoined = true;
		rel2.hasJoined = true;
	}	
}

void Statistics::BuildSubset(RelInfo &rel1, RelInfo &rel2, std::vector<std::string> &repOfSet) {
	if (!rel1.subset && !rel2.subset) {
		std::shared_ptr<Subset> subSet = std::make_shared<Subset>(repOfSet);
		rel1.subset = subSet;
		rel2.subset = subSet;
	} else if (!rel1.subset) {
		rel2.subset->push_back(repOfSet[0]);
		rel1.subset = rel2.subset;
	} else if (!rel2.subset) {
		rel1.subset->push_back(repOfSet[1]);
		rel2.subset = rel1.subset;
	} else {
		for (std::string &name : *(rel2.subset)) {
			rel1.subset->push_back(name);
		}
		rel2.subset = rel1.subset;
	}
}

double Statistics::Estimate (const struct AndList *parseTree, char **relNames, int numToJoin) {
	std::vector<std::string> repOfSet;
	//number of tuples and selectivity pair
	auto nsp = Simulate(parseTree, relNames, numToJoin, repOfSet);
	double totNumTuples = std::get<0>(nsp);
	float selectivity = std::get<1>(nsp);
	// cout << "Estimate" << totNumTuples * selectivity << endl;
	return totNumTuples * selectivity;

}

std::pair<double,float> Statistics::Simulate (const struct AndList *parseTree, char **relNames, int numToJoin, std::vector<std::string> &repOfSet) {
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
	//while there is elements in the hash table
	while (relToJoin.size() > 0) {
		auto rel = relToJoin.begin();
		auto iter = relations.find(*rel);
		repOfSet.push_back(iter->first);
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
			return EstimateSelection(parseTree, repOfSet);
		}		
	} else if (2 == numSet) {
		if (nullptr == parseTree) {
			return EstimateProduct(repOfSet);
		} else {
			return EstimateJoin(parseTree, repOfSet);
		}
	} else {
		cerr << "ERRPR: Attempt to perform operation on more than 2 relations!\n";
		exit(1);
	}
}

std::pair<double,float> Statistics::EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname) {		
	struct OrList *pOr = pAnd->left;
	struct ComparisonOp *pCom = pOr->left;
	std::string name = InitAttsName(pCom);
	RelInfo &rel = relations.at(relname[0]);
	AttInfo &atts = *(rel.atts);
	float totSel = 1.0f;
	std::vector<std::string> attNames; 
	while (pAnd != nullptr) {
		pOr = pAnd->left;
		float orSel = 1.0f;		
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			std::string name = InitAttsName(pCom);
			auto iter = atts.find(name);
			if(atts.end() == iter) {
				cerr << "ERROR: Attempt to select on non-exist attribute! " << name << "\n";
				exit(1);
			}
			int op = pCom->code;			
			float sel =  (EQUALS == op) ? 1.0f / atts[name] : 1.0f / 3;			
			SetSelectivity(name, attNames, orSel, sel);
			pOr = pOr->rightOr;
		}
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

std::string Statistics::InitAttsName (struct ComparisonOp *pCom) {
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
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME op NAME  
			if (NAME == (lOperand | rOperand)) {
				std::string lname(pCom->left->value);
				std::string rname(pCom->right->value);				
				auto numDistinctsPair = CheckAtts(lname, rname, latts, ratts);
				double lTuples = lrel.numTuples;
				double rTuples = rrel.numTuples;
				double max = std::max(numDistinctsPair.first, numDistinctsPair.second);
				if (EQUALS == op) {
					totTuples = (lTuples * rTuples) / max;
				} else {
					totTuples = (lTuples * rTuples) / (3 * max);
				}				
			}
			// NAME op value | value op name
			else if (lOperand | rOperand > 4) {
				std::string name = InitAttsName(pCom);
				//pair store the number of distinct and which side the attribute belong to
				auto DistSidePair = CheckAtts(name, latts, ratts);
				float sel =  (EQUALS == op) ? 1.0f / DistSidePair.first : 1.0f / 3;
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

std::pair<double,float> Statistics::EstimateProduct(std::vector<std::string> &relname) {
	return std::make_pair(relations.at(relname[0]).numTuples * relations.at(relname[1]).numTuples, 1);
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

std::pair<double, int> Statistics::CheckAtts(std::string &name, AttInfo &latts, AttInfo &ratts) {
	int side;
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
	return std::make_pair(iter->second, side);
}


std::pair<double, double> Statistics::CheckAtts(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts) {
	auto liter = latts.find(lname);
	auto riter = ratts.find(rname);
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
			if (rel.second.hasJoined) {
				cout << "Subset list: " << endl;
				cout << "[" << endl;
				for (auto n : *(rel.second.subset)) {
					cout << n << " ";
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
