#ifndef PARSE_INFO_H
#define PARSE_INFO_H

#include <iostream>
#include <cstring>
#include <string.h>
#include "ParseTree.h"

using namespace std;

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

void RemovePrefix (struct FuncOperator *fOperator);

void RemoveOperandPrefix (struct FuncOperand *fOperand);

void RemoveOperandPrefix (struct Operand *pOperand);

void RemovePrefixComparisonOp(struct ComparisonOp *pCom);

void RemovePrefixOrList(struct OrList *pOr);

void RemovePrefix(struct AndList *pAnd);

#endif
