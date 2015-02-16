#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <stdlib.h>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"

using namespace std;

class Run{
private:
  int runID;
  int curRunLen;
  int runBegIndex;
  int runCurIndex;
  bool exhaust;
  File *runsFile;
  Page curPage;
  Record curRec;
public:
  Run(int ID, int beg, int rl, File *rf);
  Run(const Run &r);
  ~Run();  
  Run & operator = (const Run r);
  void setExhaust();
  bool isExhaust();
  int GetRunID();
  Record GetRecord();
  int GetNext();
};

class Sorter {
private:
  OrderMaker sortorder;
  ComparisonEngine comp;
public:
  Sorter(OrderMaker s) : sortorder(s),comp() {}
  bool operator()(Record r1, Record r2){      		
    return comp.Compare (&r1, &r2, &sortorder);             
  }
};

class BigQ {
private:
  OrderMaker sortorder;
  File runsFile;
  int runNum;
  int runlen;
public:

  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();

  void TPM_MergeSort(Pipe &in, Pipe &out);
  void SortInRun(vector<Record> &oneRunRecords);
  void WriteRunToFile(vector<Record> &oneRunRecords);
  void FirstPhase(Pipe &in, vector<Run> &runs);
  void SecondPhase(Pipe &out, vector<Run> &runs);
};

#endif
