#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include <stdlib.h>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"


using namespace std;

typedef struct _QueueMember{
  int runID;
  Record rec;
}QueueMember;

class Run{
private:
  int runID;
  int curRunLen;
  int runBegIndex;
  int runCurIndex;
  File *runsFile;
  Page curPage;
public:
  Run(int ID, int beg, int rl, File *rf);
  Run(const Run &r);
  ~Run();  
  Run & operator = (const Run r);
  int GetRunID();
  int GetNext(Record &curRec);
};

class Sorter {
private:
  OrderMaker &sortorder;
  ComparisonEngine comp;
public:
  Sorter(OrderMaker s) : sortorder(s),comp() {}
  
  bool operator()(Record r1, Record r2){   
    if(comp.Compare (&r1, &r2, &sortorder) < 0){
      return true;
    }else{
      return false;
    }      
  }
  bool operator()(QueueMember &qm1, QueueMember &qm2){   
    if(comp.Compare (&(qm1.rec), &(qm2.rec), &sortorder) < 0){
      return true;
    }else{
      return false;
    }
  }
};

class BigQ {
private:
  OrderMaker sortorder;
  File runsFile;
  int runNum;
  int runlen;

  void TPM_MergeSort(Pipe &in, Pipe &out);
  void SortInRun(vector<Record> &oneRunRecords);
  void WriteRunToFile(vector<Record> &oneRunRecords);
  void FirstPhase(Pipe &in, vector<Run> &runs);
  void SecondPhase(Pipe &out, vector<Run> &runs);
public:

  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();  
};

#endif
