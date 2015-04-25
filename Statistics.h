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

#define FOUND 1
#define NOTFOUND -1

// attribute name, number of distinct value
typedef std::unordered_map<std::string,double> AttInfo;
// vector of subset name
typedef std::vector<std::string> Subset;
// number of tuples, selectivity
typedef std::pair<double,float> EstimateInfo;
// number of tuples, attribute list
struct RelInfo{
	double numTuples;
	bool hasJoined;
	AttInfo atts;
	Subset subset;
	RelInfo(double num) : numTuples(num), hasJoined(false), atts(), subset() { }
	// move constructor
	RelInfo(RelInfo && rhs) : numTuples(rhs.numTuples), hasJoined(rhs.hasJoined) {
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
	}
	RelInfo & operator = (RelInfo && rhs) {
		numTuples = rhs.numTuples;
		hasJoined = rhs.hasJoined;
		atts = std::move(rhs.atts);
		subset = std::move(rhs.subset);
		return *this;
	}
	// copy constructor deleted
	RelInfo(const RelInfo & rhs) = delete;
	RelInfo & operator =(RelInfo &rhs) = delete;
	void AddAtt(std::string attName, long numDistincts) {
		auto p = atts.emplace(attName, numDistincts);
		// if the attribute is in statisics, upadte number of distinct value of this attribute
		if (false == p.second) {
			//p.first = AttInfo::iterator point to the matched element
			(p.first)->second = numDistincts;		
		}
	}
};

// relation name, relation info: number of tuples, attribute list
typedef std::unordered_map<std::string, std::shared_ptr<RelInfo>> Relations;

class Statistics {
friend class PlanTree;
private:
	Relations relations;
	// initial selection attribute name from ComparisonOp
	std::string InitAttsName (struct ComparisonOp *pCom);
	// get number of distinct value of a attribute
	double GetAttsDistinct(std::string &name, AttInfo &latts, AttInfo &ratts);
	std::pair<double, double> GetAttsDistinct(std::string &lname, std::string &rname, AttInfo &latts, AttInfo &ratts);
	// compute selectivity
	void SetSelectivity(std::string &name, std::vector<std::string> &attNames, float &selToSet, float sel);
	// estimate the result of cross product
	EstimateInfo EstimateProduct(std::vector<std::string> &relname);
	// estimate the result of selection
	EstimateInfo EstimateSelection(const struct AndList *pAnd, std::vector<std::string> &relname);
	// estimate the result of join
	EstimateInfo EstimateJoin(const struct AndList *pAnd, std::vector<std::string> &relname);
	// simulate the operation in the parseTree and return estimate result
	EstimateInfo Simulate (const struct AndList *parseTree, char **relNames, int numToJoin, std::vector<std::string> &repOfSet);
	// find if a attribute appears in the attribute list of relName list
	int	FindAtts(char **relName, std::string &AttsName, int relToJoin) const;
	// give a joined relation a new name and delete all the old names
	void RenameJoinedRel(std::string &newName, std::vector<char*> &relNames);
	
public:
	Statistics();
	Statistics(Statistics const &copyMe);	 // Performs deep copy
	// add a relation to the statistics
	void AddRel(char *relName, long numTuples);
	// add an attribute to the statistics
	void AddAtt(char *relName, char *attName, long numDistincts);
	// copy relation
	void CopyRel(char *oldName, char *newName);
	// read statistics from file
	void Read(const char *fromWhere);
	// write statistics from file
	void Write(const char *fromWhere);
	// apply the result of performing parseTree
	void Apply(const struct AndList *parseTree, char **relNames, int numToJoin);
	// estimate the result of performing parseTree
	double Estimate(const struct AndList *parseTree, char **relNames, int numToJoin);
	// print the statistics
	void Print() const;
	// get number of tuples of a relation
	int GetNumTuples(const std::string &relName) const;
	// check if the statistics contain a relation
	int CheckRels(const char* relName) const;
};

#endif
