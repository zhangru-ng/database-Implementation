#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <utility>
#include <string>

using std::cerr;
using std::vector;
using std::unordered_map;
using std::pair;
using std::make_pair;
using std::string;
using std::get;
using std::ofstream;
using std::ifstream;
using std::endl;

typedef unordered_map<string,double> Attributes;
typedef pair<double, Attributes> Info;
typedef unordered_map<string, Info> Relations;

class Statistics
{
private:
	Relations relations;
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
