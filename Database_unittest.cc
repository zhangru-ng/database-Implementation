#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include "DBFile.h"
#include "BigQ.h"
#include "Pipe.h"
#include "gtest/gtest.h"

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern FILE * yyin;
extern struct AndList *final;

char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl"; 
char tbl_name[50] = "lineitem";
char catalog_path[100] = "catalog";
char testFile_path[100] = "dbfile/testFile.bin";
char headFile_path[100] = "dbfile/testFile.header";

/*
 * Heap file unit test
 * Rui 2015.02.06	
 */ 
 /*
class HeapFileTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		dbfile.Create(testFile_path, heap , 0);
	}

	virtual void TearDown() {
		dbfile.Close();
		remove(testFile_path);
		remove(headFile_path);
	}
	DBFile dbfile;		
};

TEST_F(HeapFileTest, CreateFile) {
	FILE * fp = fopen(testFile_path,"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(HeapFileTest, OpenFile) {
	int f_flag = dbfile.Open(testFile_path);
	EXPECT_EQ( 1 , f_flag );	
}

TEST_F(HeapFileTest, CloseFile) {
 	EXPECT_EQ(1 , dbfile.Close() );     //close created file
	dbfile.Open(testFile_path); 
	EXPECT_EQ(1 , dbfile.Close() );		//close open file
}

TEST_F(HeapFileTest, CreateHeapTypeHeader) {
	char type[8];
	FILE * fp = fopen(headFile_path,"r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
	EXPECT_STREQ("heap", type);	
}

TEST_F(HeapFileTest, LoadFile) {
	Schema f_schema(catalog_path, tbl_name);
	EXPECT_EQ( 0, dbfile.curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir);
	EXPECT_LE( 2, dbfile.curFile.GetLength() );
}

TEST_F(HeapFileTest, MoveFirst) {
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.curPageIndex );
}

TEST_F(HeapFileTest, GetNextRecord) {
	Record tempRec1, tempRec2;
	Schema f_schema(catalog_path, tbl_name);
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec2) && tempRec1.SuckNextRecord (&f_schema, tableFile) ){
		EXPECT_EQ( 0, strcmp( tempRec1.GetBits(), tempRec2.GetBits() ) );
	}
	fclose(tableFile);
}

TEST_F(HeapFileTest, AddRecord) {
	char *tempBits = NULL;
	Record tempRec1;
		
	EXPECT_EQ( 0, dbfile.curFile.GetLength() );
	tempBits = new (std::nothrow) char[PAGE_SIZE/2];
	ASSERT_TRUE(tempBits != NULL);
	((int *) tempBits)[0] = PAGE_SIZE/2;
	tempRec1.SetBits(tempBits);
	dbfile.Add (tempRec1);
	EXPECT_EQ( 2, dbfile.curFile.GetLength() );
	
	tempBits = new (std::nothrow) char[PAGE_SIZE/4];
	ASSERT_TRUE(tempBits != NULL);
	((int *) tempBits)[0] = PAGE_SIZE/4;
	tempRec1.SetBits(tempBits); //SetBits is pointer operation and Add
	dbfile.Add (tempRec1);		//consume rec, namely consume tempBits
	EXPECT_EQ( 2, dbfile.curFile.GetLength() );		
	
	tempBits = new (std::nothrow) char[PAGE_SIZE/4];
	ASSERT_TRUE(tempBits != NULL);
	((int *) tempBits)[0] = PAGE_SIZE/4;
	tempRec1.SetBits(tempBits); 
	dbfile.Add (tempRec1);		
	EXPECT_EQ( 3, dbfile.curFile.GetLength() );		
//	delete[] tempBits;
}


TEST(HeapFileDeathTest, OpenNonExistFile) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Open("dbfile/nonexist.bin"), "BAD!  Open did not work for dbfile/nonexist.bin");	
}

TEST(HeapFileDeathTest, CreateInNonExistDir) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Create("nonexist/testFile.bin", heap , 0), "BAD!  Open did not work for nonexist/testFile.bin");
}

TEST(HeapFileDeathTest, AddRecordLTPage) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	char *tempBits = NULL;
	Record tempRec1;
	tempBits = new (std::nothrow) char[PAGE_SIZE];
	ASSERT_TRUE(tempBits != NULL);
	((int *) tempBits)[0] = PAGE_SIZE;
	tempRec1.SetBits(tempBits); 
	EXPECT_DEATH( dbfile.Add (tempRec1), "This record is larger than a DBFile page");
}
*/
/*
 * Big queue unit test
 * Rui 2015.02.21	
 */ 

 char sort_cnf_dir[100] = "sortCNF/";

 typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	int errnum;
}testutil;

void *producer (void *arg) {
	Pipe *myPipe = (Pipe *) arg;
	Record temp;
	int counter = 0;
	DBFile tdbfile;
	tdbfile.Create(testFile_path, heap , 0);
	Schema f_schema(catalog_path, tbl_name);
	tdbfile.Load (f_schema, tbl_dir);
	tdbfile.MoveFirst ();
	while (tdbfile.GetNext (temp) == 1) {
		myPipe->Insert (&temp);
	}
	tdbfile.Close ();
	myPipe->ShutDown ();
	pthread_exit(NULL);
}

void *consumer (void *arg) {	
	testutil *t = (testutil *) arg;
	ComparisonEngine ceng;
	int err = 0;
	int i = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];
		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
		}
		i++;	
	}
	t->errnum = err;
	pthread_exit(NULL);
}

void initOrderMaker(OrderMaker &sortorder, char* sortname, char* tbl_name){
	char sortfile[100];
	sprintf (sortfile, "%s%s", sort_cnf_dir , sortname);
	FILE *fp = fopen(sortfile, "r");
	// make sure it is valid:
	if (!fp) {
		cout << "Can't open" << sortfile << endl;
		exit(1);
	}
	// set lex to read from it instead of defaulting to STDIN:
	yyin = fp;
	// parse through the input until there is no more:
	do {
		if(yyparse()){
			cerr << "Can't parse test sort CNF";
			exit(1);
		}
	} while (!feof(yyin));
	Record literal;
	CNF sort_pred;
	Schema *lisc = new Schema (catalog_path, tbl_name);
	sort_pred.GrowFromParseTree (final, lisc, literal); // constructs CNF predicate
	OrderMaker dummy;
	sort_pred.GetSortOrders (sortorder, dummy);
	fclose(fp);
}

void initTestRun(vector<Record> &oneRunRecords){
	Record tempRec;
	char sort_tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/nation.tbl"; 
	char sort_tbl_name[100] = "nation";
	Schema f_schema(catalog_path, sort_tbl_name);
	FILE *tableFile = fopen (sort_tbl_dir, "r");
	assert(tableFile != NULL);		
	while (tempRec.SuckNextRecord (f_schema, tableFile)){
		oneRunRecords.push_back(tempRec);
	}
	fclose(tableFile);
}

class BigQTest : public ::testing::Test {
protected:
	virtual void SetUp(){
		buffsz = 100;
		runlen = 10;
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);		
		pthread_create (&thread1, NULL, producer, (void *)input);		
		tutil.pipe = output;
		tutil.order = &sortorder;
		tutil.errnum = 0;	
	}
	virtual void TearDown() {
		//pthread_join (thread1, NULL);
		//pthread_join (thread2, NULL);
		delete input;
		delete output;
		remove(testFile_path);
		remove(headFile_path);
	}	
	int buffsz;
	Pipe *input;
  	Pipe *output;
	OrderMaker sortorder;
	int runlen;	
	pthread_t thread1;
	pthread_t thread2;	
	testutil tutil; 
	ComparisonEngine ceng;
	DBFile dbfile;
};

TEST_F(BigQTest, SortIntAttribute) {
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}
/*
TEST_F(BigQTest, SortDoubleAttribute) {
	initOrderMaker(sortorder, "sort2", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortStringAttribute) {
	initOrderMaker(sortorder, "sort3", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortTwoLiterals) {
	initOrderMaker(sortorder, "sort4", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortThreeLiterals) {
	initOrderMaker(sortorder, "sort5", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortSixLiterals) {
	initOrderMaker(sortorder, "sort6", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthOne) {
	runlen = 1;
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwo) {
	runlen = 2;
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthSeven) {
	runlen = 7;
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwenty) {
	runlen = 20;
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthFifty) {
	runlen = 50;
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	EXPECT_EQ( 0, tutil.errnum );
}
*/

TEST_F(BigQTest, SortInRun) {
	int err = 0;
	Record tempRec[2];
	vector<Record> oneRunRecords;	

	initOrderMaker(sortorder, "sortmem", sort_tbl_name);
	BigQ bq (*input, *output, sortorder, 0);

	initTestRun(oneRunRecords);
	bq.SortInRun(oneRunRecords);
	while(oneRunRecords.size() > 1){
		tempRec[0] = oneRunRecords.back();
		oneRunRecords.pop_back();
		tempRec[1] = oneRunRecords.back();
		if (ceng.Compare (&tempRec[0], &tempRec[1], &sortorder) < 0) {
				err++;
		}
	}
	EXPECT_EQ( 0, err );
}

TEST_F(BigQTest, WriteRunToFile) {
	int err = 0, i = 0;
	int beg, len;
	Record tempRec[2];
	Record *last = NULL, *prev = NULL;
	vector<Record> oneRunRecords;
		
	initOrderMaker(sortorder, "sortmem", sort_tbl_name);
	BigQ bq (*input, *output, sortorder, 0);
		
	initTestRun(oneRunRecords);
	bq.SortInRun(oneRunRecords);
	bq.runsFileName="/cise/tmp/rui/testfile.bin";
	//create the associate file for temp file to avoid error
	dbfile.Create(bq.runsFileName, heap, 0);	
	dbfile.Close();
	
	bq.runsFile.Open(1, bq.runsFileName);
	bq.WriteRunToFile(oneRunRecords, beg, len);
	bq.runsFile.Close();

	dbfile.Open(bq.runsFileName);
	dbfile.MoveFirst();
	while(dbfile.GetNext(tempRec[i%2])){
		prev = last;
		last = &tempRec[i%2];
		if (prev && last) {
			if (ceng.Compare (prev, last, &sortorder) == 1) {
				err++;
			}
		}
		i++;	
	}	
	dbfile.Close();
	
	EXPECT_EQ( 0, err );
	EXPECT_EQ( 0, beg );
	EXPECT_EQ( 1, len );
	
	remove(bq.runsFileName);	
}
