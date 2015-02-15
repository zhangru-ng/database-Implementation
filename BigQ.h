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

using namespace std;


class Sorter {
    private:
       OrderMaker sortorder;
       ComparisonEngine comp;
	public:
      Sorter(OrderMaker s) : sortorder(s),comp() {}
      bool operator()(Record const &r1, Record const &r2){      		
			return comp.Compare (&r1, &r2, &orderUs);             
      }
};

class BigQ {
private:
	File runsFile;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
