#include "Statistics.h"

Statistics::Statistics () {
	relations.reserve(8);
}

Statistics::Statistics (Statistics &copyMe) {
	relations = copyMe.relations;
}

// typedef unordered_map<string,double> Attributes;
// typedef pair<double, Attributes> Info;
// typedef unordered_map<string, Info> Relations;

void Statistics::AddRel (char *relName, int numTuples) {
	// if the relation name equal to NULL
	if (nullptr == relName) {
		cerr << "ERROR: Invalid relation name in AddRel\n";
		exit(1);
	}
	// if the relation is not in statisics
	if (relations.find(relName) == relations.end()) {
		// add this relation in
		Attributes emptyAtts;	
		Info info = make_pair(numTuples, emptyAtts);
		relations.emplace(relName, info);
	} else {
		// otherwise upadte number of tuples of this relation
		get<0>(relations.at(relName)) = numTuples;
	}	
}

void Statistics::AddAtt (char *relName, char *attName, int numDistincts) {
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
	auto &atts = get<1>(relations.at(relName));
	// if this attribute is not in attribute list
	if (atts.find(attName) == atts.end()){
		// add it into attribute list
		atts.emplace(attName, numDistincts);
	} else {
		// otherwise update number of distinct values of this attribute
		atts.at(attName) = numDistincts;		
	}
}

void Statistics::CopyRel (char *oldName, char *newName) {
	// if the old relation is not in statisics, report error and exit
	if (relations.find(oldName) == relations.end()) {
		cerr << "ERROR: Attempt to copy non-exist relation!\n";
		exit(1);
	} else {
		//copy old relation infomation
		Info info = make_pair(get<0>(relations.at(oldName)), get<1>(relations.at(oldName)));
		//Add into relations with new name
		relations.emplace(newName, info);
	}	
}
	
void Statistics::Read (const char *fromWhere) {
	ifstream infile(fromWhere);
	if (!infile.is_open()) {
		return;
	}
	string relName;
	string attName;
	double numTuples, numDistincts;
	Attributes atts;
	atts.reserve(16);
	while (infile.good()) {
		infile >> relName;
		infile >> numTuples;		
		while (1) {
			infile >> attName;
			if ("REL_END" == attName) {
				break;
			}
			infile >> numDistincts;
			atts.emplace(attName, numDistincts);
		}
		Info info = make_pair(numTuples, atts);
		relations.emplace(relName, info);
		atts.clear();
	}
}

void Statistics::Write (const char *fromWhere) {
	ofstream outfile(fromWhere);
	if (!outfile.is_open()) {
		cerr << "ERROR: Can not oper output file in Statistics::Write\n";
		exit(1);
	}
	for (auto rel : relations) {
		outfile << rel.first << " " << get<0>(rel.second) << endl;
		for (auto atts : get<1>(rel.second)) {
			outfile << atts.first << " " << atts.second << endl;
		}
		outfile << "REL_END" <<endl;
		outfile << endl;
	}
}

void  Statistics::Apply (struct AndList *parseTree, char *relNames[], int numToJoin) {

}

double Statistics::Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {

}

