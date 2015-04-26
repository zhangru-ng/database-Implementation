#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <list>
#include <utility>
#include <algorithm>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "Thread.h"

using std::vector;
using std::string;
using std::cerr;
using std::cout;
using std::endl;

#define THRESHOLD 30

class Run;
//struct to store priority queue member
class QueueMember{
public:
    int runID;        //run ID which the record from 
    Record rec;       //current first record
    QueueMember();      
    QueueMember(QueueMember &&qm);
    QueueMember & operator = (QueueMember &&qm);
};

class LinearScanMember{
public:
    Run *myRun;       //bind a run to this list member
    Record rec;       //current first record
    LinearScanMember();
    LinearScanMember(LinearScanMember &&lm);
    LinearScanMember & operator = (LinearScanMember &&lm);
};

class BigQ : public Thread {
private:
    Pipe &in;
    Pipe &out;
    OrderMaker &sortorder;
    int runlen;
    string runsFileName;
    File runsFile;
    int runNum;
    pthread_t workthread;
   // static void* workerthread_wrapper (void* arg);
    void *InternalThreadEntry();
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
    ~BigQ();
};

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
    //delete default copy constructor and assignment to avoid incorrect copy
    Run(const Run &r) = delete;
    Run & operator = (const Run &r) = delete;
    //move constructor and move assignment
    Run (Run &&r);
    Run & operator = (Run &&r);
    //get the ID of this run
    int GetRunID();
    //rewind to first record of this run
    void MoveFirst();
    //get the next record of this run 
    int GetNext(Record &curRec);
};

//Record compare class
class Sorter {
private:
    OrderMaker &sortorder;
    ComparisonEngine comp;
public:
    Sorter(OrderMaker &s) : sortorder(s),comp() {}
    //compare operator for phase two priority queue
    bool operator()(const QueueMember &qm1, const QueueMember &qm2) const {   
      return comp.Compare (&(qm1.rec), &(qm2.rec), &sortorder) > 0;
    }
};

class Queue {
private:
    vector<QueueMember> qm;     //internal container 
    Sorter &sorter;             //comparison class
public:
    Queue(Sorter &s);
    //reserve place in vector to increase performance
    void reserve(int n);
    //return if the queue if empty
    bool empty();
    //push element into queue
    void push(QueueMember &element);
    //pop element from queue
    QueueMember pop();
};

#endif
