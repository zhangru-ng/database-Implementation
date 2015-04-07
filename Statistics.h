#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <string>
#include <vector>
#include <memory>
#include <algorithm>
using std::cout;
using std::cerr;
using std::endl;

// attribute name, number of distinct value
typedef std::unordered_map<std::string,long> AttInfo;

typedef struct _set {
	double numTuples;
	AttInfo atts;
	std::vector<std::string> names;
	_set(double num) : numTuples(num), atts(), names() { }
} Subset;

// number of tuples, attribute list
typedef struct info {
	long numTuples;
	AttInfo atts;
	bool hasJoined;
	std::shared_ptr<Subset> sp;
	info(long num) : numTuples(num), atts(), hasJoined(false), sp(nullptr) { }
	info(info && rhs) : numTuples(rhs.numTuples), hasJoined(rhs.hasJoined) {
		atts = std::move(rhs.atts);
		sp = std::move(rhs.sp);
	}
	info(const info& rhs) : numTuples(rhs.numTuples), atts(rhs.atts), sp(rhs.sp), hasJoined(rhs.hasJoined) { }
} RelInfo;

// relation name, relation info: number of tuples, attribute list
typedef std::unordered_map<std::string, RelInfo> Relations;

class Statistics {
private:
	Relations relations;
	std::unordered_map<std::string, float> sizeParameters;
	
	std::string initAttsName (struct ComparisonOp *pCom);
	std::pair<double,float> EstimateProduct(std::vector<std::string> &relname);
	std::pair<double,float> EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname);
	std::pair<double,float> EstimateJoin(const struct AndList *pAnd, std::vector<std::string> &relname);
	std::pair<double,float> Simulate (const struct AndList *parseTree, char **relNames, int numToJoin);
	
public:
	Statistics();
	Statistics(Statistics const &copyMe);	 // Performs deep copy

	void AddRel(char *relName, long numTuples);
	void AddAtt(char *relName, char *attName, long numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);

	void Apply(const struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(const struct AndList *parseTree, char **relNames, int numToJoin);
	
	void Print() const;

	
};

#endif
