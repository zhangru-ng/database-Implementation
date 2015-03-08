#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include "DBFile.h"
#include "BigQ.h"
#include "gtest/gtest.h"

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern FILE * yyin;
extern struct AndList *final;

char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl"; 
char tbl_name[50] = "lineitem";
char catalog_path[100] = "catalog";
char cnf_dir[100] = "filterCNF/";
char testFile_path[100] = "/cise/tmp/rui/testFile.bin";
char headFile_path[100] = "/cise/tmp/rui/testFile.bin.header";

/*
 * Heap DBFile unit test
 * Written by Rui 2015.02.06	
 */ 
 
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
	ComparisonEngine comp;	
	CNF cnf_pred;
	Record literal;
public:
	int AddRecord(int size);
};

int HeapFileTest::AddRecord(int size){
	char *tempBits = NULL;
	Record tempRec;

	tempBits = new (std::nothrow) char[size];
	if(tempBits == NULL){
		return 0;
	}
	((int *) tempBits)[0] = size;
	tempRec.SetBits(tempBits); //SetBits is pointer operation and Add
	dbfile.Add (tempRec);		//consume rec, namely consume tempBits
	return 1;
}

void initCNF(CNF &cnf_pred, Record &literal, char* cnf_name, char* tbl_name){
	char cnf_file[100];
	sprintf (cnf_file, "%s%s", cnf_dir , cnf_name);
	FILE *fp = fopen(cnf_file, "r");
	// make sure it is valid:
	if (!fp) {
		cout << "Can't open" << cnf_file << endl;
		exit(1);
	}
	// set lex to read from it instead of defaulting to STDIN:
	yyin = fp;
	// parse through the input until there is no more:
	do {
		if(yyparse()){
			cerr << "Can't parse test filter CNF";
			exit(1);
		}
	} while (!feof(yyin));
	Schema lisc(catalog_path, tbl_name);
	cnf_pred.GrowFromParseTree (final, &lisc, literal); // constructs CNF predicate
	fclose(fp);
}

TEST_F(HeapFileTest, CreateFile) {
	FILE * fp = fopen(testFile_path,"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(HeapFileTest, CreateWrongType){
	int what = 10;
	EXPECT_EQ( 0, dbfile.Create(testFile_path, (fType)what , 0) );
}

TEST_F(HeapFileTest, CreateInNonExistDir) {
	EXPECT_EQ( 0, dbfile.Create("nonexist/testFile.bin", heap , 0) );
}

TEST_F(HeapFileTest, OpenFile) {
	EXPECT_EQ( 1 , dbfile.Close() );     //close created file
	EXPECT_EQ( 1 , dbfile.Open(testFile_path) );	
}

TEST_F(HeapFileTest, OpenNonExistFile) {
	EXPECT_EQ( 0, dbfile.Open("dbfile/nonexist.bin") );	
}

TEST_F(HeapFileTest, OpenWithoutHeaderFile) {
	remove(headFile_path);
	EXPECT_EQ( 0 , dbfile.Open(testFile_path) );	
}

TEST_F(HeapFileTest, CloseFile) {
 	EXPECT_EQ( 1 , dbfile.Close() );     //close created file
	dbfile.Open(testFile_path); 
	EXPECT_EQ( 1 , dbfile.Close() );		//close open file
}

TEST_F(HeapFileTest, CreateHeapTypeHeader) {
	int type;
	FILE * fp = fopen(headFile_path,"r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%d", &type);
	fclose(fp);		
	EXPECT_EQ(0, type);	
}

TEST_F(HeapFileTest, LoadFile) {
	Schema f_schema(catalog_path, tbl_name);
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir);
	EXPECT_LE( 2, dbfile.myInernalPoniter->curFile.GetLength() );
}

TEST_F(HeapFileTest, MoveFirst) {
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curPageIndex );
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
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curFile.GetLength() );
	ASSERT_TRUE(AddRecord(PAGE_SIZE/2));	
	EXPECT_EQ( 2, dbfile.myInernalPoniter->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	EXPECT_EQ( 2, dbfile.myInernalPoniter->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	EXPECT_EQ( 3, dbfile.myInernalPoniter->curFile.GetLength() );
}

TEST_F(HeapFileTest, GetNextFilterInt) {
	int err = 0;
	Record tempRec;

	Schema f_schema(catalog_path, tbl_name);
	initCNF(cnf_pred, literal, "cnf1", tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(HeapFileTest, GetNextFilterDouble) {
	int err = 0;
	Record tempRec;

	Schema f_schema(catalog_path, tbl_name);
	initCNF(cnf_pred, literal, "cnf2", tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(HeapFileTest, GetNextFilterString) {
	int err = 0;
	Record tempRec;

	Schema f_schema(catalog_path, tbl_name);
	initCNF(cnf_pred, literal, "cnf3", tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(HeapFileTest, GetNextFilterMixed1) {
	int err = 0;
	Record tempRec;

	Schema f_schema(catalog_path, tbl_name);
	initCNF(cnf_pred, literal, "cnf4", tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(HeapFileTest, GetNextFilterMixed2) {
	int err = 0;
	Record tempRec;

	Schema f_schema(catalog_path, tbl_name);
	initCNF(cnf_pred, literal, "cnf5", tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST(HeapFileDeathTest, AddRecordLargerThanPage) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	dbfile.Create(testFile_path, heap , 0);
	char *tempBits = NULL;
	Record tempRec1;
	tempBits = new (std::nothrow) char[PAGE_SIZE];
	ASSERT_TRUE(tempBits != NULL);
	((int *) tempBits)[0] = PAGE_SIZE;
	tempRec1.SetBits(tempBits); 
	EXPECT_DEATH( dbfile.Add (tempRec1), "This record is larger than a DBFile page");
}

/*
 * Big queue unit test
 * Written by Rui 2015.02.21	
 */ 

char sort_cnf_dir[100] = "sortCNF/";

 typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	int errnum;
}testutil;

class BigQTest : public ::testing::Test {
protected:
	virtual void SetUp(){
		buffsz = 100;
		runlen = 10;
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);		
		//pthread_create (&thread1, NULL, producer, (void *)input);		
		tutil.pipe = output;
		tutil.order = &sortorder;
		tutil.errnum = 0;	
	}
	virtual void TearDown() {
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

void *producer (void *arg) {
	Pipe *myPipe = (Pipe *) arg;
	Record temp;
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
	//pthread_exit(NULL);
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
	//pthread_exit(NULL);
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
	Schema lisc(catalog_path, tbl_name);
	sort_pred.GrowFromParseTree (final, &lisc, literal); // constructs CNF predicate
	OrderMaker dummy;
	sort_pred.GetSortOrders (sortorder, dummy);
	fclose(fp);
}

TEST_F(BigQTest, SortIntAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortDoubleAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort2", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortStringAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort3", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortTwoLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort4", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortThreeLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort5", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortSixLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort6", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthOne) {
	runlen = 1;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwo) {
	runlen = 2;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthSeven) {
	runlen = 7;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwenty) {
	runlen = 20;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthFifty) {
	runlen = 50;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	initOrderMaker(sortorder, "sort1", tbl_name);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}


/*
 * Sorted DBFile unit test
 * Written by Rui 2015.03.08	
 */ 

class SortedFileTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		runlen = 10;
		initOrderMaker(sortorder, "sort1", tbl_name);
		startup.order = &sortorder;  //can be initailized as startup = {&sortorder, runlen}; in C++ 11
		startup.runlen = runlen;
	}

	virtual void TearDown() {
		dbfile.Close();
		remove(testFile_path);
		remove(headFile_path);
	}
	DBFile dbfile;	
	ComparisonEngine comp;	
	CNF cnf_pred;
	Record literal;
	int runlen;
	OrderMaker sortorder;
	SortInfo startup;
public:
	int checkOrder(OrderMaker &sortorder);
};

int SortedFileTest::checkOrder(OrderMaker &sortorder){
	ComparisonEngine ceng;
	int err = 0;
	int i = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	while (dbfile.GetNext(rec[i%2])) {
		prev = last;
		last = &rec[i%2];
		if (prev && last) {
			if (ceng.Compare (prev, last, &sortorder) == 1) {
				err++;
			}
		}
		i++;	
	}
	return err;
}

TEST_F(SortedFileTest, CreateFile){
	dbfile.Create(testFile_path, sorted , &startup);
	FILE * fp = fopen(testFile_path,"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(SortedFileTest, SortedTypeHeaderFile){
	dbfile.Create(testFile_path, sorted , &startup);
	int type;
	FILE * fp = fopen(headFile_path,"r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%d", &type);
	fclose(fp);		
	EXPECT_EQ(1, type);	
}

TEST_F(SortedFileTest, CreateWithWrongOrder){
	startup.order = NULL;
	startup.runlen = runlen;
	EXPECT_EQ( 0, dbfile.Create(testFile_path, sorted , &startup) );	
}

TEST_F(SortedFileTest, CreateWithWrongRunlen){
	startup.order = &sortorder;
	startup.runlen = -2;
	EXPECT_EQ( 0, dbfile.Create(testFile_path, sorted , &startup) );
}

TEST_F(SortedFileTest, CreateInNonExistDir) {
	EXPECT_EQ( 0, dbfile.Create("nonexist/testFile.bin", sorted , &startup) );
}

TEST_F(SortedFileTest, OpenFile) {
	dbfile.Create(testFile_path, sorted , &startup);
	dbfile.Close();
	EXPECT_EQ( 1 , dbfile.Open(testFile_path) );	
}

TEST_F(SortedFileTest, OpenNonExistFile) {
	EXPECT_EQ( 0, dbfile.Open("dbfile/nonexist.bin") );	
}

TEST_F(SortedFileTest, CloseFile) {
	dbfile.Create(testFile_path, sorted , &startup);
 	EXPECT_EQ(1 , dbfile.Close() );     //close created file
	dbfile.Open(testFile_path); 
	EXPECT_EQ(1 , dbfile.Close() );		//close open file
}

TEST_F(SortedFileTest, MoveFirst) {
	dbfile.Create(testFile_path, sorted , &startup);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curPageIndex );
}

TEST_F(SortedFileTest, AddRecord) {
	Record tempRec;
	Schema f_schema(catalog_path, tbl_name);
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);	
	dbfile.Create(testFile_path, sorted , &startup);		
	while (tempRec.SuckNextRecord (&f_schema, tableFile)){
		dbfile.Add(tempRec);
	}
	fclose(tableFile);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, LoadFile) {
	dbfile.Create(testFile_path, sorted , &startup);
	Schema f_schema(catalog_path, tbl_name);
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_LE( 2, dbfile.myInernalPoniter->curFile.GetLength() );
}

TEST_F(SortedFileTest, GetNextRecord) {
	dbfile.Create(testFile_path, sorted , &startup);	
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}	

TEST_F(SortedFileTest, SortOneLiterals) {			
	initOrderMaker(sortorder, "sort2", tbl_name);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortTwoLiterals) {		
	initOrderMaker(sortorder, "sort4", tbl_name);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortSixLiterals) {		
	initOrderMaker(sortorder, "sort6", tbl_name);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);
	Schema f_schema(catalog_path, tbl_name);	
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, GetNextFilterMixed1) {
	int err = 0;
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	initCNF(cnf_pred, literal, "cnf4", tbl_name);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, GetNextFilterMixed2) {
	int err = 0;
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Create(testFile_path, sorted , &startup);		
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	initCNF(cnf_pred, literal, "cnf5", tbl_name);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}


TEST_F(SortedFileTest, FilterMatchSortAtts) {
	initOrderMaker(sortorder, "sort7", tbl_name);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	initCNF(cnf_pred, literal, "cnf6", tbl_name);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, FilterNotMatchSortAtts) {	
	dbfile.Create(testFile_path, sorted , &startup);	
	Schema f_schema(catalog_path, tbl_name);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	initCNF(cnf_pred, literal, "cnf6", tbl_name);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}
