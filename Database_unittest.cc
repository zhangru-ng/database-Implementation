#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "gtest/gtest.h"

char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl"; 
char tbl_name[50] = "lineitem";
char catalog_path[100] = "catalog";
char testFile_path[100] = "/cise/tmp/rui/testFile.bin";
char headFile_path[100] = "/cise/tmp/rui/testFile.bin.header";

extern "C" {
	int yyparse (void);   // defined in y.tab.c
	int yyfuncparse (void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
}
extern FILE * yyin;
extern struct AndList *final;
extern struct FuncOperator *finalfunc;

/*
 * Heap DBFile unit test
 * Written by Rui 2015.02.06	
 */ 
 void get_cnf (char *input, Schema *f_schema, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, f_schema, literal); // constructs CNF predicate
	close_lexical_parser ();
}
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

TEST_F(HeapFileTest, OpenAndCloseFile) {
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
	dbfile.MoveFirst();
	EXPECT_EQ( 2, dbfile.myInernalPoniter->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	dbfile.MoveFirst();
	EXPECT_EQ( 2, dbfile.myInernalPoniter->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	dbfile.MoveFirst();
	EXPECT_EQ( 3, dbfile.myInernalPoniter->curFile.GetLength() );
}

TEST_F(HeapFileTest, AddAndGetRecord) {
	Record tempRec;
	Schema f_schema(catalog_path, tbl_name);
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	int count1 = 0;
	while(tempRec.SuckNextRecord (&f_schema, tableFile)){
		dbfile.Add(tempRec);
		++count1;
	}
	dbfile.MoveFirst();
	int count2 = 0;
	while (dbfile.GetNext(tempRec)){
		++count2;
	}
	fclose(tableFile);
	EXPECT_EQ( count1, count2); 
}

TEST_F(HeapFileTest, GetNextFilterInt) {
	int err = 0;
	Record tempRec;
	Schema f_schema(catalog_path, tbl_name);
	get_cnf ("(l_orderkey < 27)", &f_schema, cnf_pred, literal);
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
	get_cnf ("(l_extendedprice > 15000.0) AND (l_extendedprice < 17000.0)", &f_schema, cnf_pred, literal);
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
	get_cnf ("(l_comment > 'c') AND (l_comment < 'e')", &f_schema, cnf_pred, literal);
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
	get_cnf ("(l_orderkey > 100) AND (l_orderkey < 1000) AND (l_partkey > 100) AND (l_partkey < 5000) AND (l_shipmode = 'AIR') AND (l_linestatus = 'F') AND (l_tax < 0.07)", &f_schema, cnf_pred, literal);
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
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", &f_schema, cnf_pred, literal);
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


TEST(HeapFileDeathTest, CloseTwice) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	dbfile.Create(testFile_path, heap , 0);
	dbfile.Close();
	EXPECT_DEATH( dbfile.Close(), "ERROR: DBFile is not initialized or is closed twice!");
}*/

/*
 * Big queue unit test
 * Written by Rui 2015.02.21	
 */ 

void get_sort_order (char *input, Schema *f_schema, OrderMaker &sortorder) {		
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	Record literal;
	CNF sort_pred;
	sort_pred.GrowFromParseTree (final, f_schema, literal); // constructs CNF predicate
	OrderMaker dummy;
	sort_pred.GetSortOrders (sortorder, dummy);
	close_lexical_parser ();
}

/*
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
		f_schema = new Schema(catalog_path, tbl_name);
		tutil.pipe = output;
		tutil.order = &sortorder;
		tutil.errnum = 0;	
	}
	virtual void TearDown() {
		delete input;
		delete output;
		delete f_schema;
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
	Schema *f_schema;
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
}

TEST_F(BigQTest, SortIntAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);	
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortDoubleAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);
	get_sort_order ("(l_quantity)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortStringAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)input);	
	get_sort_order ("(l_shipdate)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortTwoLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);	
	get_sort_order ("(l_partkey) AND (l_extendedprice)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortThreeLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);	
	get_sort_order ("(l_suppkey) AND (l_discount) AND (l_returnflag)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortSixLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey) AND (l_shipmode) AND (l_commitdate) AND (l_receiptdate) AND (l_shipinstruct) AND (l_tax)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthOne) {
	runlen = 1;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwo) {
	runlen = 2;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthSeven) {
	runlen = 7;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthTwenty) {
	runlen = 20;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthFifty) {
	runlen = 50;
	pthread_create (&thread1, NULL, producer, (void *)input);		
	get_sort_order ("(l_suppkey)", f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}*/


/*
 * Sorted DBFile unit test
 * Written by Rui 2015.03.08	
 */ 
/*
class SortedFileTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		runlen = 10;
		f_schema = new Schema(catalog_path, tbl_name);
		get_sort_order ("(l_suppkey)", f_schema, sortorder);
		startup.order = &sortorder;  //can be initailized as startup = {&sortorder, runlen}; in C++ 11
		startup.runlen = runlen;		
	}

	virtual void TearDown() {
		delete f_schema;
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
	Schema *f_schema;
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

TEST_F(SortedFileTest, OpenAndCloseFile) {
	dbfile.Create(testFile_path, sorted , &startup);
	EXPECT_EQ(1 , dbfile.Close() ); 
	EXPECT_EQ( 1 , dbfile.Open(testFile_path) );	
}

TEST_F(SortedFileTest, OpenNonExistFile) {	
	EXPECT_EQ( 0, dbfile.Open("dbfile/nonexist.bin") );
	EXPECT_EQ( 1 , dbfile.Create(testFile_path, sorted , &startup));	
}

TEST_F(SortedFileTest, MoveFirst) {
	dbfile.Create(testFile_path, sorted , &startup);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curPageIndex );
}

TEST_F(SortedFileTest, AddRecord) {
	Record tempRec;
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);	
	dbfile.Create(testFile_path, sorted , &startup);		
	while (tempRec.SuckNextRecord (f_schema, tableFile)){
		dbfile.Add(tempRec);
	}
	fclose(tableFile);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, LoadFile) {
	dbfile.Create(testFile_path, sorted , &startup);
	EXPECT_EQ( 0, dbfile.myInernalPoniter->curFile.GetLength() );
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_LE( 2, dbfile.myInernalPoniter->curFile.GetLength() );
}

TEST_F(SortedFileTest, GetNextRecord) {
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}	

TEST_F(SortedFileTest, SortOneLiterals) {			
	get_sort_order ("(l_quantity)", f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortTwoLiterals) {		
	get_sort_order ("(l_partkey) AND (l_extendedprice)", f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortSixLiterals) {		
	get_sort_order ("(l_suppkey) AND (l_shipmode) AND (l_commitdate) AND (l_receiptdate) AND (l_shipinstruct) AND (l_tax)", f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, GetNextFilterMixed1) {
	int err = 0;
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	get_cnf ("(l_orderkey > 100) AND (l_orderkey < 1000) AND (l_partkey > 100) AND (l_partkey < 5000) AND (l_shipmode = 'AIR') AND (l_linestatus = 'F') AND (l_tax < 0.07)", f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, GetNextFilterMixed2) {
	int err = 0;
	dbfile.Create(testFile_path, sorted , &startup);		
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}


TEST_F(SortedFileTest, FilterMatchSortAtts) {
	get_sort_order (" (l_quantity)", f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, FilterNotMatchSortAtts) {	
	dbfile.Create(testFile_path, sorted , &startup);	
	dbfile.Load (*f_schema, tbl_dir);
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}*/

/*
 * RelOp unit test
 * Written by Rui 2015.03.20	
 */ 
void get_cnf (char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, right, literal); // constructs CNF predicate
	close_lexical_parser ();
}

void get_cnf (char *input, Schema *left, Function &fn_pred) {
	init_lexical_parser_func (input);
		if (yyfuncparse() != 0) {
		cout << " Error: can't parse your arithmetic expr. " << input << endl;
		exit (1);
	}
	fn_pred.GrowFromParseTree (finalfunc, *left); // constructs CNF predicate
	close_lexical_parser_func ();
}

class RelOpTest : public ::testing::Test {
protected:
	virtual void SetUp() {
		dbfile.Create(testFile_path, heap , 0);
		f_schema = new Schema(catalog_path, tbl_name);
		dbfile.Load (*f_schema, tbl_dir);
		
	}

	virtual void TearDown() {
		dbfile.Close();
		delete f_schema;
		remove(testFile_path);
		remove(headFile_path);
	}
	DBFile dbfile;
	Schema *f_schema;	
	ComparisonEngine comp;	
	OrderMaker sortorder;
	static const int numpgs = 100;
public:
	int clear_pipe(Pipe &input, CNF &cnf_pred, Record &literal);
	int check_atts_num (Pipe &input, int numAtts);
	int check_tuple_num(Pipe &input);
};

int RelOpTest::clear_pipe (Pipe &input, CNF &cnf_pred, Record &literal){
	Record rec;
	int err = 0;
	while (input.Remove (&rec)) {
		if (0 == comp.Compare(&rec, &literal, &cnf_pred)) {
			err++;
		}	
	}
	return err;
}

int RelOpTest::check_atts_num (Pipe &input, int numAtts){
	Record rec;
	int err = 0;
	while (input.Remove (&rec)) {
		if (rec.GetNumAtts() != numAtts) {
			err++;
		}	
	}
	return err;
}

int RelOpTest::check_tuple_num(Pipe &input){
	Record rec;
	int count = 0;
	while (input.Remove (&rec)) {
		++count;	
	}
	return count;
}




TEST_F(RelOpTest, SelectFile1) {
	CNF cnf_pred;
	Record literal;	
	SelectFile SF;	
	get_cnf ("(l_orderkey < 27)", f_schema, cnf_pred, literal);
	Pipe output(100);
	SF.Run (dbfile, output, cnf_pred, literal);
	int err =  clear_pipe (output, cnf_pred, literal);
	SF.WaitUntilDone ();
	EXPECT_EQ( 0, err );
}


TEST_F(RelOpTest, SelectFile2) {
	CNF cnf_pred;
	Record literal;
	SelectFile SF;	
	get_cnf ("(l_partkey < 5000) AND (l_shipmode = 'AIR') AND (l_linestatus = 'F')", f_schema, cnf_pred, literal);
	Pipe output(100);
	SF.Run (dbfile, output, cnf_pred, literal);
	int err =  clear_pipe (output, cnf_pred, literal);
	SF.WaitUntilDone ();
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, SelectPipe1) {
	CNF sf_pred, sp_pred;
	Record lit_sf, lit_sp;
	SelectFile SF;
	SelectPipe SP;	
	get_cnf ("(l_orderkey < 1000)", f_schema, sf_pred, lit_sf);
	Pipe li(100);
	SF.Run (dbfile, li, sf_pred, lit_sf);
	get_cnf ("(l_orderkey < 100)", f_schema, sp_pred, lit_sp);
	Pipe output(100);
	SP.Run (li, output, sp_pred, lit_sp);
	int err =  clear_pipe (output, sp_pred, lit_sp);
	SF.WaitUntilDone();
	SP.WaitUntilDone();
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, SelectPipe2) {
	CNF sf_pred, sp_pred, out_pred;
	Record lit_sf, lit_sp, lit_out;
	SelectFile SF;
	SelectPipe SP;	
	get_cnf ("(l_partkey < 5000)", f_schema, sf_pred, lit_sf);
	Pipe li(100);
	SF.Run (dbfile, li, sf_pred, lit_sf);
	get_cnf ("(l_shipmode = 'AIR')", f_schema, sp_pred, lit_sp);
	Pipe output(100);
	SP.Run (li, output, sp_pred, lit_sp);
	get_cnf ("(l_partkey < 5000) AND (l_shipmode = 'AIR')", f_schema, out_pred, lit_out);	
	int err =  clear_pipe (output, out_pred, lit_out);
	SF.WaitUntilDone();
	SP.WaitUntilDone();
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, Project1) {
	CNF sf_pred, p_pred;
	Record lit_sf, lit_p;
	SelectFile SF;
	get_cnf ("(l_orderkey < 1000) AND (l_orderkey >800)", f_schema, sf_pred, lit_sf);
	Pipe li(100);
	SF.Run (dbfile, li, sf_pred, lit_sf);
	Project P;
		Pipe output(100);
		int keepMe[] = {0,1,5};
		int numAttsIn = 16;
		int numAttsOut = 3;
	P.Run (li, output, keepMe, numAttsIn, numAttsOut);
	int err = check_atts_num(output, numAttsOut);
	SF.WaitUntilDone();
	P.WaitUntilDone();
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, Project2) {
	CNF sf_pred, p_pred;
	Record lit_sf, lit_p;
	SelectFile SF;
	get_cnf ("(l_partkey < 2000)", f_schema, sf_pred, lit_sf);
	Pipe li(100);
	SF.Run (dbfile, li, sf_pred, lit_sf);
	Project P;
		Pipe output(100);
		int keepMe[] = {0,1,5,7,9};
		int numAttsIn = 16;
		int numAttsOut = 5;
	P.Run (li, output, keepMe, numAttsIn, numAttsOut);
	int err = check_atts_num(output, numAttsOut);
	SF.WaitUntilDone();
	P.WaitUntilDone();
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, DuplicateRemoval) {
	CNF sf_pred, p_pred;
	Record lit_sf, lit_p;
	SelectFile SF;
	get_cnf ("(l_orderkey = l_orderkey)", f_schema, sf_pred, lit_sf);
	Pipe li(100);
	SF.Run (dbfile, li, sf_pred, lit_sf);
	Project P;
		int keepMe[] = {3};
		int numAttsIn = 16;
		int numAttsOut = 1;
		Pipe pl(100);
	P.Run (li, pl, keepMe, numAttsIn, numAttsOut);
	DuplicateRemoval D;
		// inpipe = __ps
		Pipe output(100);
		Attribute IA = {"l_linenumber", Int};
		Schema out_sch ("test", 1, &IA);
	D.Run (pl, output, out_sch);
	int count = check_tuple_num(output);
	SF.WaitUntilDone();
	P.WaitUntilDone();
	D.WaitUntilDone();
	EXPECT_EQ( 7, count );
}