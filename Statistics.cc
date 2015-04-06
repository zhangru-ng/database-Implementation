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
		std::terminate();
	}
	// add this relation in
	auto p = relations.emplace(relName, RelInfo(numTuples, relName));
	// if the relation is in statisics, upadte number of tuples of this relation
	if (false == p.second) {
		//p.first = Relations::iterator
		p.first->second.numTuples = numTuples;		
	} 
}

void Statistics::AddAtt (char *relName, char *attName, long numDistincts) {
	//if the names are invalid, report and exit
	if (nullptr == relName || nullptr == attName) {
		cerr << "ERROR: Invalid relation name or attribute name in AddAtt\n";
		std::terminate();
	}
	// if the relation is not in statisics, report error and exit
	if (relations.find(relName) == relations.end()) {
		cerr << "ERROR: Attempt to add attribute to non-exist relation!\n";
		std::terminate();
	} 
	// obtain attribute list of this relation
	auto &atts = relations.at(relName).atts;
	auto p = atts.emplace(attName, numDistincts);
	// if the attribute is in statisics, upadte number of distinct value of this attribute
	if (false == p.second) {
		//p.first = AttInfo::iterator
		p.first->second = numDistincts;		
	}
}

void Statistics::CopyRel (char *oldName, char *newName) {
	//Add into relations with new name
	auto p = relations.emplace(newName, RelInfo(relations.at(oldName)));
	// if the old relation is not in statisics, report error and exit
	if (false == p.second) {
		cerr << "ERROR: Attempt to copy non-exist relation!\n";
		std::terminate();
	}	
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
		RelInfo info(numTuples, relName);	
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
		std::terminate();
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
	


}

long Statistics::Estimate (const struct AndList *parseTree, char **relNames, int numToJoin) {

}

long Statistics::Simulate (const struct AndList *parseTree, char **relNames, int numToJoin) {
	std::unordered_set<std::string> relToJoin;
	for (int i = 0; i < numToJoin; i++) {
		//if the relations to join is not in current statistics, report error and exit
		if (relations.find(relNames[i]) == relations.end()) {
			cerr << "ERROR: Current statistics does not contain " << relNames << endl;
			std::terminate();
		}
		//otherwis hash them into a hash table relToJoin
		relToJoin.emplace(relNames[i]);
	}
	int numSet = 0;
	//while there is elements in the hash table
	while (relToJoin.size() > 0) {
		auto rel = relToJoin.begin();
		//get the subset list of first element in the table
		std::vector<std::string> &subset = relations.find(*rel)->second.subset;
		//erase this element in the hash table after get its subset member
		relToJoin.erase(rel);
		//for each elements in the subset 
		for (auto const &srel : subset) {
			//if one of the elements in the subset is not in relations to join, this join does not make sense
			//because all relation in the subset list have been joined into one relation
			if (relToJoin.find(srel) == relToJoin.end()) {
				cerr << "ERROR: Attempt to join incomplete subset members\n";
				std::terminate();
			} else {
				//erase the matched relation
				relToJoin.erase(rel);
			}
		}
		++numSet;
	}
	if (1 == numSet) {
		if (nullptr == parseTree) {
			// do noting
		} else {
			// seletion
		}		
	} else {
		if (nullptr == parseTree) {
			// cross product
		} else {
			// (join | seletion)*
		}
		
	}
}

// #define LESS_THAN 1
// #define GREATER_THAN 2
// #define EQUALS 3

// // these are the types of operands that can appear in a CNF expression
// #define DOUBLE 1
// #define INT 2
// #define STRING 3
// #define NAME 4

// struct Operand {
// 	int code;
// 	char *value;
// };
// struct ComparisonOp {
// 	int code;
// 	struct Operand *left;
// 	struct Operand *right;	
// };

void Statistics::EstimateOrList(struct AndList *pAnd) {
	while (pAnd != nullptr) {
		struct OrList *pOr = pAnd->left;
		std::unordered_map<std::string, float> seletivities;
		while (pOr != nullptr) {
			struct ComparisonOp *pCom = pOr->left;
			int op = pCom->code;
			int lOperand = pCom->left->code;
			int rOperand = pCom->right->code;
			// NAME = NAME
			if (EQUALS == op && NAME == lOperand | rOperand) {
				std::string lname(pCom->left->value);
				std::string rname(pCom->right->value);
				auto lrel = FindAtts(lname);
				auto rrel = FindAtts(rname);
				seletivities.emplace(std::make_pair(lrel.fisrt, 1.0));
				seletivities.emplace(std::make_pair(rrel.fisrt, 1.0));
			}
			// NAME , value | value , name
			else if (lOperand | rOperand > 4) {
				if (lOperand == NAME) {
					std::string name(pCom->left->value);				
				} else {
					std::string name(pCom->right->value);
				}
				auto rel = FindAtts(name);
				AttInfo &atts = rel.second.atts;
				float seletivity =  EQUALS == op ? 1 / atts.at(name) : 1.0 / 3;
				auto p = seletivities.emplace(std::make_pair(rel.fisrt, seletivity));
				if (false == p.second) {
					float s = p.first->second;
					//seletivity
					p.first->second = s == 1 ? selectivity : 1 - (1 - seletivity) * (1 - s);
				}
			}
			pOr = pOr->rightOr;
		}
		for (auto s : seletivities) {
			//pair<iterator, bool>
			auto ibp = sizeParameters.emplace(std::make_pair(s.first, s.second));
			if (false == ibp.second) {
				//selectivity multiplication
				ibp.first->second *= s.second;
			}
		}
		pAnd = pAnd->rightAnd
	}
	
}

const Relations& Statistics::FindAtts(std::string &name) {
	for (auto const &rel : relations) {
		const AttInfo &atts = rel.second.atts;
		if (atts.end() != atts.find(name)) {
			return rel;
		}
	}
	cerr << "ERROR: Attempt to join relations using non-exist attribute!\n";
	std::terminate();
}

long Statistics::SimulateJoin(struct ComparisonOp *pCom) {

}

long Statistics::SimulateSeletion(struct ComparisonOp *pCom) {

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