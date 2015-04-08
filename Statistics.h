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
// attribute name, number of distinct value
typedef std::unordered_map<std::string,double> AttInfo;
typedef std::vector<std::string> Subset;
// number of tuples, attribute list
typedef struct _RelInfo{
	double numTuples;
	bool hasJoined;
	std::shared_ptr<AttInfo> atts;
	std::shared_ptr<Subset> subset;
	_RelInfo(double num) : numTuples(num), hasJoined(false), atts(nullptr), subset() { }
	_RelInfo(_RelInfo && rhs) : numTuples(rhs.numTuples), hasJoined(rhs.hasJoined) {
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
	}
	_RelInfo& operator = (_RelInfo && rhs) {
		numTuples = rhs.numTuples;
		hasJoined = rhs.hasJoined;
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
		return *this;
	}
	_RelInfo(const _RelInfo& rhs) : numTuples(rhs.numTuples), hasJoined(rhs.hasJoined), atts(rhs.atts), subset(rhs.subset) { }
	_RelInfo & operator =(_RelInfo &rhs) {
		numTuples = rhs.numTuples;
		hasJoined = rhs.hasJoined;
		atts = rhs.atts;
		subset = rhs.subset;
		return *this;
	}
	void AddAtt(std::string attName, long numDistincts) {
		if(nullptr == atts) {
			atts = std::make_shared<AttInfo>();
			atts->emplace(attName, numDistincts);
		} else {
			auto p = atts->emplace(attName, numDistincts);
			// if the attribute is in statisics, upadte number of distinct value of this attribute
			if (false == p.second) {
				//p.first = AttInfo::iterator point to the matched element
				(p.first)->second = numDistincts;		
			}
		}
	}
}RelInfo;

// relation name, relation info: number of tuples, attribute list
typedef std::unordered_map<std::string, RelInfo> Relations;

class Statistics {
private:
	Relations relations;
	std::unordered_map<std::string, float> sizeParameters;
	std::string initAttsName (struct ComparisonOp *pCom);
	double CheckAtts(std::string &name, AttInfo &latts, AttInfo &ratts);
	std::pair<double, double> CheckAtts(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts);
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
