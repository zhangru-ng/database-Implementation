#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "Thread.h"



using namespace std;

//struct to store priority queue member
typedef struct _QueueMember{
  int runID;        //run ID which the record from 
  Record rec;       
}QueueMember;

class Run{
private:
  int runID;            //Id of this run
  int curRunLen;        //length of this run 
  int runBegIndex;      //begin page index of this run in runsFile
  int runCurIndex;      //current page index of this run in runsFile
  File *runsFile;       //point to the temporary file storing runs
  Page curPage;         //current page being read
public:
  Run(int ID, int beg, int rl, File *rf);
  Run(const Run &r);
  ~Run();  
  Run & operator = (const Run r);
  //get the ID of this run
  int GetRunID();
  //get the next record of this run 
  int GetNext(Record &curRec);
};

//Record compare class
class Sorter {
private:
  OrderMaker &sortorder;
  ComparisonEngine comp;
public:
  Sorter(OrderMaker s) : sortorder(s),comp() {}
  //compare operator for phase one vecter<Record>
  bool operator()(const Record &r1, const Record &r2){   
    if(comp.Compare (&r1, &r2, &sortorder) < 0){
      return true;
    }else{
      return false;
    }      
  }
  //compare operator for phase two priority queue
  bool operator()(const QueueMember &qm1, const QueueMember &qm2){   
    if(comp.Compare (&(qm1.rec), &(qm2.rec), &sortorder) > 0){
      return true;
    }else{
      return false;
    }
  }
};

class BigQ : public Thread {
private:
  Pipe &in;
  Pipe &out;
  OrderMaker &sortorder;
  int runlen;
  char *runsFileName;
  File runsFile;
  int runNum;
  pthread_t workthread;
  //bootstrap wrapper function
 // static void* workerthread_wrapper (void* arg);
  void *InternalThreadEntry();
  //two phase multiway merge sort main function
  // void * TPM_MergeSort();
  //sort one run record in first phase
  void SortInRun(vector<Record> &oneRunRecords);
  //write one run to temporary file, store the begin and length of current run
  void WriteRunToFile(vector<Record> &oneRunRecords, int &beg, int &len);  
  //First phase of TPMMS
  void FirstPhase(vector<Run> &runs);
  //Second phase of TPMMS
  void SecondPhase(vector<Run> &runs);

  void PriorityQueue(vector<Run> &runs);
  void LinearScan(vector<Run> &runs);  

public:
  BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
  ~BigQ ();  
};

#endif
