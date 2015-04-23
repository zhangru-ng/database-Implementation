#ifndef PARSE_INFO_H
#define PARSE_INFO_H

#include <iostream>
#include <cstring>
#include <string.h>
#include "ParseTree.h"

#include <unordered_set>
#include <vector>

using namespace std;

typedef struct AndList* Predicate;

void print_Operand(struct Operand *pOperand);

void print_ComparisonOp(struct ComparisonOp *pCom);

void print_OrList(struct OrList *pOr);

void print_AndList(struct AndList *pAnd);

void print_predicate(struct AndList *pAnd);

void print_table_list(struct TableList *tables);

void print_FuncOperand (struct FuncOperand *fOperand);

void print_FuncOperator (struct FuncOperator *fOperator);

void print_aggregate(struct FuncOperator *fOperator);

void print_distinct(int distinctAtts, int distinctFunc);

void print_name_list(struct NameList *names);

void print_groupAtts(struct NameList *names);

void print_selectAtts(struct NameList *names);

// remove relation name prefix in sum predicate
void RemovePrefix (struct FuncOperator *fOperator);

void RemoveOperandPrefix (struct FuncOperand *fOperand);

void RemoveOperandPrefix (struct Operand *pOperand);

void RemovePrefixComparisonOp(struct ComparisonOp *pCom);

void RemovePrefixOrList(struct OrList *pOr);

// remove relation name prefix in where predicate
void RemovePrefix(struct AndList *pAnd);

// init default select file predicate for relation
void InitDefaultPredicate(std::string attName, Predicate &initPred);

// check if the sum prediacte is legal
void CheckSumPredicate(struct FuncOperator *finalFunction, struct NameList *groupingAtts, struct NameList *attsToSelect);

// check if all the select attribute are in grouping list
void CheckGroupAndSelect(struct NameList *groupingAtts, struct NameList *attsToSelect);

// check if the sum distinct attribute in grouping list
void CheckDistinctFunc(struct FuncOperator *finalFunction, struct NameList *groupingAtts);

void CheckFuncOperand (struct FuncOperand *fOperand, std::unordered_set<std::string> &names);

void CheckFuncOperator (struct FuncOperator *fOperator, std::unordered_set<std::string> &names);

// get the attribute list in SUM
struct NameList* GetSumAtts(struct FuncOperator *fOperator);

void GetSumOperandAtts (struct FuncOperand *fOperand, std::vector<char*> &sumAtts);

void GetSumOperatorAtts(struct FuncOperator *fOperator, std::vector<char*> &sumAtts);

void DestroyNameList(struct NameList *names);

#endif