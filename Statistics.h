#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <exception>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <string>
#include <vector>

using std::cout;
using std::cerr;
using std::endl;

// attribute name, number of distinct value
typedef std::unordered_map<std::string,long> AttInfo;
// number of tuples, attribute list
typedef struct info{
	long numTuples;
	AttInfo atts;
	std::vector<std::string> subset;
	info(long num, std::string name) : numTuples(num), atts(), subset{name} { }
	info(info && rhs) : numTuples(rhs.numTuples) {
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
	}
	info(const info& rhs) : numTuples(rhs.numTuples), atts(rhs.atts), subset(rhs.subset) { }
}RelInfo;
// relation name, relation info: number of tuples, attribute list
typedef std::unordered_map<std::string, RelInfo> Relations;

// typedef struct einfo{
// 	float selectivity; 
// 	AttInfo atts;	
// }EstInfo;

class Statistics
{
private:
	Relations relations;
	std::unordered_map<std::string, float> sizeParameters;
	void EstimateAndList(struct AndList *pAnd);
	long SimulateJoin(struct ComparisonOp *pCom);
	long SimulateSeletion(struct ComparisonOp *pCom);
	long Simulate (const struct AndList *parseTree, char **relNames, int numToJoin);
	const Relations& FindAtts(std::string &name);

public:
	Statistics();
	Statistics(Statistics const &copyMe);	 // Performs deep copy

	void AddRel(char *relName, long numTuples);
	void AddAtt(char *relName, char *attName, long numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);

	void Apply(const struct AndList *parseTree, char *relNames[], int numToJoin);
	long Estimate(const struct AndList *parseTree, char **relNames, int numToJoin);
	
	void Print() const;
};

#endif
