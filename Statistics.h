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

#define UNKNOWN -1
#define LEFT 0
#define RIGHT 1

// attribute name, number of distinct value
typedef std::unordered_map<std::string,double> AttInfo;
typedef std::vector<std::string> Subset;
typedef std::pair<double,float> EstimateInfo;
// number of tuples, attribute list
typedef struct RelInfo_{
	double numTuples;
	bool hasJoined;
	AttInfo atts;
	Subset subset;
	RelInfo_(double num) : numTuples(num), hasJoined(false), atts(), subset() { }
	RelInfo_(RelInfo_ && rhs) : numTuples(rhs.numTuples), hasJoined(rhs.hasJoined) {
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
	}
	RelInfo_ & operator = (RelInfo_ && rhs) {
		numTuples = rhs.numTuples;
		hasJoined = rhs.hasJoined;
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
		return *this;
	}
	RelInfo_(const RelInfo_ & rhs) = delete;
	RelInfo_ & operator =(RelInfo_ &rhs) = delete;
	void AddAtt(std::string attName, long numDistincts) {
		auto p = atts.emplace(attName, numDistincts);
		// if the attribute is in statisics, upadte number of distinct value of this attribute
		if (false == p.second) {
			//p.first = AttInfo::iterator point to the matched element
			(p.first)->second = numDistincts;		
		}
	}
}RelInfo;

// relation name, relation info: number of tuples, attribute list
typedef std::unordered_map<std::string, std::shared_ptr<RelInfo>> Relations;

class Statistics {
friend class PlanTree;
private:
	Relations relations;
	std::unordered_map<std::string, float> sizeParameters;
	std::string InitAttsName (struct ComparisonOp *pCom);
	double CheckAtts(std::string &name, AttInfo &latts, AttInfo &ratts);
	std::pair<double, double> CheckAtts(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts);
	void SetSelectivity(std::string &name, std::vector<std::string> &attNames, float &selToSet, float sel);
	void BuildSubset(RelInfo &rel1, RelInfo &rel2, std::vector<std::string> &repOfSet); 
	EstimateInfo EstimateProduct(std::vector<std::string> &relname);
	EstimateInfo EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname);
	EstimateInfo EstimateJoin(const struct AndList *pAnd, std::vector<std::string> &relname);
	EstimateInfo Simulate (const struct AndList *parseTree, char **relNames, int numToJoin, std::vector<std::string> &repOfSet);
	int	FindAtts(char **relName, std::string &AttsName, int relToJoin) const;
	void CheckRels(const char* relName) const;
	
public:
	Statistics();
	Statistics(Statistics const &copyMe);	 // Performs deep copy

	void AddRel(char *relName, long numTuples);
	void AddAtt(char *relName, char *attName, long numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);

	void Apply(const struct AndList *parseTree, char **relNames, int numToJoin);
	double Estimate(const struct AndList *parseTree, char **relNames, int numToJoin);
	
	void Print() const;
	
};

#endif
