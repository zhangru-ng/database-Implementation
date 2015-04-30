#include <limits.h>
#include <stdio.h>
#include <assert.h>
#include "DBFile.h"
#include "BigQ.h"
#include "RelOp.h"
#include "Statistics.h"
#include "ParseTree.h"
#include "gtest/gtest.h"

string tbl_prefix = "/cise/homes/rui/Desktop/testfiles/10M_table/";
string tbl_name[8] = {"region", "nation", "customer", "part", "partsupp", "supplier", "order", "lineitem"};
typedef enum table { REGION = 0, NATION, CUSTOMER, PART, PARTSUPP, SUPPLIER, ORDER, LINEITEM } Table;
string catalog_path = "catalog";
string testFile_path[2] = { "dbfile/testFile1.bin", "dbfile/testFile2.bin" };
string headFile_path[2] = { "dbfile/testFile1.bin.header", "dbfile/testFile2.bin" };

extern "C" {
	int yycnfparse (void);   // defined in y.tab.c
	int yycnfparse(void);	// defined in yycnf.tab.c
	int yyfuncparse (void);   // defined in yyfunc.tab.c
	void init_lexical_parser_cnf (char *);	// defined in lex.yycnf.tab.c
	void close_lexical_parser_cnf ();	// defined in lex.yycnf.tab.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
	struct YY_BUFFER_STATE *yycnf_scan_string(const char*);
}
extern FILE * yyin;
extern struct AndList *final;
extern struct FuncOperator *finalfunc;

/*
 * Heap DBFile unit test
 * Written by Rui 2015.02.06	
 */ 
 void get_cnf (char *input, Schema *f_schema, CNF &cnf_pred, Record &literal) {
	init_lexical_parser_cnf (input);
  	if (yycnfparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, f_schema, literal); // constructs CNF predicate
	close_lexical_parser_cnf ();
}

class HeapFileTest : public ::testing::Test {
protected:
	virtual void SetUp() {		
		dbfile.Create(testFile_path[0].c_str(), heap , 0);
	}

	virtual void TearDown() {
		dbfile.Close();
		remove(testFile_path[0].c_str());
		remove(headFile_path[0].c_str());
	}
	DBFile dbfile;	
	ComparisonEngine comp;	
	CNF cnf_pred;
	Record literal;
	static Schema f_schema;
	static string tbl_dir;
public:
	int AddRecord(int size);
};

Schema HeapFileTest::f_schema = { catalog_path.c_str(), tbl_name[(int)LINEITEM].c_str() };
string HeapFileTest::tbl_dir = tbl_prefix + tbl_name[(int)LINEITEM] + ".tbl";

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
	FILE * fp = fopen(testFile_path[0].c_str(),"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(HeapFileTest, CreateWrongType){
	int what = 10;
	EXPECT_EQ( 0, dbfile.Create(testFile_path[0].c_str(), (fType)what , 0) );
}

TEST_F(HeapFileTest, CreateInNonExistDir) {
	EXPECT_EQ( 0, dbfile.Create("nonexist/testFile.bin", heap , 0) );
}

TEST_F(HeapFileTest, OpenAndCloseFile) {
	EXPECT_EQ( 1 , dbfile.Close() );     //close created file
	EXPECT_EQ( 1 , dbfile.Open(testFile_path[0].c_str()) );	
}

TEST_F(HeapFileTest, OpenNonExistFile) {
	EXPECT_EQ( 0, dbfile.Open("dbfile/nonexist.bin") );	
}

TEST_F(HeapFileTest, OpenWithoutHeaderFile) {
	remove(headFile_path[0].c_str());
	EXPECT_EQ( 0 , dbfile.Open(testFile_path[0].c_str()) );	
}

TEST_F(HeapFileTest, CreateHeapTypeHeader) {
	int type;
	FILE * fp = fopen(headFile_path[0].c_str(),"r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%d", &type);
	fclose(fp);		
	EXPECT_EQ(0, type);	
}

TEST_F(HeapFileTest, LoadFile) {
	EXPECT_EQ( 0, dbfile.myInternalPointer->curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir.c_str());
	EXPECT_LE( 2, dbfile.myInternalPointer->curFile.GetLength() );
}

TEST_F(HeapFileTest, MoveFirst) {
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.myInternalPointer->curPageIndex );
}

TEST_F(HeapFileTest, GetNextRecord) {
	Record tempRec1, tempRec2;
	
	FILE *tableFile = fopen (tbl_dir.c_str(), "r");
	ASSERT_TRUE(tableFile != NULL);
	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec2) && tempRec1.SuckNextRecord (&f_schema, tableFile) ){
		EXPECT_EQ( 0, strcmp( tempRec1.GetBits(), tempRec2.GetBits() ) );
	}
	fclose(tableFile);
}

TEST_F(HeapFileTest, AddRecord) {
	EXPECT_EQ( 0, dbfile.myInternalPointer->curFile.GetLength() );
	ASSERT_TRUE(AddRecord(PAGE_SIZE/2));	
	dbfile.MoveFirst();
	EXPECT_EQ( 2, dbfile.myInternalPointer->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	dbfile.MoveFirst();
	EXPECT_EQ( 2, dbfile.myInternalPointer->curFile.GetLength() );
	
	ASSERT_TRUE(AddRecord(PAGE_SIZE/4));	
	dbfile.MoveFirst();
	EXPECT_EQ( 3, dbfile.myInternalPointer->curFile.GetLength() );
}

TEST_F(HeapFileTest, AddAndGetRecord) {
	Record tempRec;
	FILE *tableFile = fopen (tbl_dir.c_str(), "r");
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
	get_cnf ("(l_orderkey < 27)", &f_schema, cnf_pred, literal);
	dbfile.Load (f_schema, tbl_dir.c_str());
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
	get_cnf ("(l_extendedprice > 15000.0) AND (l_extendedprice < 17000.0)", &f_schema, cnf_pred, literal);
	dbfile.Load (f_schema, tbl_dir.c_str());
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
	get_cnf ("(l_comment > 'c') AND (l_comment < 'e')", &f_schema, cnf_pred, literal);
	dbfile.Load (f_schema, tbl_dir.c_str());
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
	get_cnf ("(l_orderkey > 100) AND (l_orderkey < 1000) AND (l_partkey > 100) AND (l_partkey < 5000) AND (l_shipmode = 'AIR') AND (l_linestatus = 'F') AND (l_tax < 0.07)", &f_schema, cnf_pred, literal);
	dbfile.Load (f_schema, tbl_dir.c_str());
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
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", &f_schema, cnf_pred, literal);
	dbfile.Load (f_schema, tbl_dir.c_str());
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
	dbfile.Create(testFile_path[0].c_str(), heap , 0);
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

void get_sort_order (char *input, Schema *f_schema, OrderMaker &sortorder) {		
	init_lexical_parser_cnf (input);
  	if (yycnfparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	Record literal;
	CNF sort_pred;
	sort_pred.GrowFromParseTree (final, f_schema, literal); // constructs CNF predicate
	OrderMaker dummy;
	sort_pred.GetSortOrders (sortorder, dummy);
	close_lexical_parser_cnf ();
}


 typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	int errnum;
}testutil;

typedef struct {
	Pipe *pipe;
	DBFile *dbfile;
}testinput;

class BigQTest : public ::testing::Test {
protected:
	static void SetUpTestCase() {
		dbfile.Create(testFile_path[0].c_str(), heap , 0);
		dbfile.Load (f_schema, tbl_dir.c_str());			
	}
	static void TearDownTestCase() {
		dbfile.Close();
		remove(testFile_path[0].c_str());
		remove(headFile_path[0].c_str());
	}

	virtual void SetUp(){	
		dbfile.MoveFirst ();	
		input = new Pipe(buffsz);
		output = new Pipe(buffsz);	
		tinput.pipe = input;	
		tinput.dbfile = &dbfile;
		tutil.pipe = output;
		tutil.order = &sortorder;
		tutil.errnum = 0;	
	}
	virtual void TearDown() {
		delete input;
		delete output;
		
	}	
	static int buffsz;
	static int runlen;
	static DBFile dbfile;
	static Schema f_schema;
	static string tbl_dir;
	ComparisonEngine ceng;
	Pipe *input;
  	Pipe *output;
	OrderMaker sortorder;	
	pthread_t thread1;
	pthread_t thread2;	
	testutil tutil;
	testinput tinput;	
};

int BigQTest::buffsz = 100;
int BigQTest::runlen = 10;
string BigQTest::tbl_dir = tbl_prefix + tbl_name[(int)LINEITEM] + ".tbl";
Schema BigQTest::f_schema = {catalog_path.c_str(), tbl_name[(int)LINEITEM] .c_str()};
DBFile BigQTest::dbfile;


void *producer (void *arg) {
	testinput *i = (testinput *)arg;
	Pipe *myPipe = i->pipe;
	DBFile *dbfile = i->dbfile;
	Record temp;	
	while (dbfile->GetNext (temp) == 1) {
		myPipe->Insert (&temp);
	}
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
	pthread_create (&thread1, NULL, producer, (void *)&tinput);
	get_sort_order ("(l_suppkey)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortDoubleAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)&tinput);
	get_sort_order ("(l_quantity)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortStringAttribute) {
	pthread_create (&thread1, NULL, producer, (void *)&tinput);	
	get_sort_order ("(l_shipdate)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortTwoLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)&tinput);	
	get_sort_order ("(l_partkey) AND (l_extendedprice)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortThreeLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)&tinput);	
	get_sort_order ("(l_suppkey) AND (l_discount) AND (l_returnflag)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, SortSixLiterals) {
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_suppkey) AND (l_shipmode) AND (l_commitdate) AND (l_receiptdate) AND (l_shipinstruct) AND (l_tax)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthOne) {
	runlen = 1;
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_suppkey)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthThree) {
	runlen = 3;
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_orderkey)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}


TEST_F(BigQTest, RunLengthSeven) {
	runlen = 7;
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_tax)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}


TEST_F(BigQTest, RunLengthFifteen) {
	runlen = 15;
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_comment)", &f_schema, sortorder);
	pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	BigQ bq (*input, *output, sortorder, runlen);	
	pthread_join (thread1, NULL);
	pthread_join (thread2, NULL);
	EXPECT_EQ( 0, tutil.errnum );
}

TEST_F(BigQTest, RunLengthFifty) {
	runlen = 50;
	pthread_create (&thread1, NULL, producer, (void *)&tinput);		
	get_sort_order ("(l_shipdate)", &f_schema, sortorder);
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
		get_sort_order ("(l_suppkey)", &f_schema, sortorder);
		startup.order = &sortorder;  //can be initailized as startup = {&sortorder, runlen}; in C++ 11
		startup.runlen = runlen;		
	}

	virtual void TearDown() {
		dbfile.Close();
		remove(testFile_path[0].c_str());
		remove(headFile_path[0].c_str());
	}	
	ComparisonEngine comp;	
	CNF cnf_pred;
	Record literal;	
	OrderMaker sortorder;
	SortInfo startup;
	DBFile dbfile;
	static int runlen;
	static Schema f_schema;
	static string tbl_dir;
public:
	int checkOrder(OrderMaker &sortorder);
};
int SortedFileTest::runlen = 10;	
Schema SortedFileTest::f_schema = { catalog_path.c_str(), tbl_name[(int)LINEITEM] .c_str() };
string SortedFileTest::tbl_dir = tbl_prefix + tbl_name[(int)LINEITEM] + ".tbl";

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
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	FILE * fp = fopen(testFile_path[0].c_str(),"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(SortedFileTest, SortedTypeHeaderFile){
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	int type;
	FILE * fp = fopen(headFile_path[0].c_str(),"r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%d", &type);
	fclose(fp);		
	EXPECT_EQ(1, type);	
}

TEST_F(SortedFileTest, CreateWithWrongOrder){
	startup.order = NULL;
	startup.runlen = runlen;
	EXPECT_EQ( 0, dbfile.Create(testFile_path[0].c_str(), sorted , &startup) );	
}

TEST_F(SortedFileTest, CreateWithWrongRunlen){
	startup.order = &sortorder;
	startup.runlen = -2;
	EXPECT_EQ( 0, dbfile.Create(testFile_path[0].c_str(), sorted , &startup) );
}

TEST_F(SortedFileTest, CreateInNonExistDir) {
	EXPECT_EQ( 0, dbfile.Create("nonexist/testFile.bin", sorted , &startup) );
}

TEST_F(SortedFileTest, OpenAndCloseFile) {
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	EXPECT_EQ(1 , dbfile.Close() ); 
	EXPECT_EQ( 1 , dbfile.Open(testFile_path[0].c_str()) );	
}

TEST_F(SortedFileTest, OpenNonExistFile) {	
	EXPECT_EQ( 0, dbfile.Open("dbfile/nonexist.bin") );
	EXPECT_EQ( 1 , dbfile.Create(testFile_path[0].c_str(), sorted , &startup));	
}

TEST_F(SortedFileTest, MoveFirst) {
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, dbfile.myInternalPointer->curPageIndex );
}

TEST_F(SortedFileTest, AddRecord) {
	Record tempRec;
	FILE *tableFile = fopen (tbl_dir.c_str(), "r");
	ASSERT_TRUE(tableFile != NULL);	
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);		
	while (tempRec.SuckNextRecord (&f_schema, tableFile)){
		dbfile.Add(tempRec);
	}
	fclose(tableFile);
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, LoadFile) {
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	EXPECT_EQ( 0, dbfile.myInternalPointer->curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	EXPECT_LE( 2, dbfile.myInternalPointer->curFile.GetLength() );
}

TEST_F(SortedFileTest, GetNextRecord) {
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}	

TEST_F(SortedFileTest, SortOneLiterals) {			
	get_sort_order ("(l_quantity)", &f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortTwoLiterals) {		
	get_sort_order ("(l_partkey) AND (l_extendedprice)", &f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, SortSixLiterals) {		
	get_sort_order ("(l_suppkey) AND (l_shipmode) AND (l_commitdate) AND (l_receiptdate) AND (l_shipinstruct) AND (l_tax)", &f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();
	EXPECT_EQ( 0, checkOrder(sortorder) );
}

TEST_F(SortedFileTest, GetNextFilterMixed1) {
	int err = 0;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();	
	Record tempRec;
	get_cnf ("(l_orderkey > 100) AND (l_orderkey < 1000) AND (l_partkey > 100) AND (l_partkey < 5000) AND (l_shipmode = 'AIR') AND (l_linestatus = 'F') AND (l_tax < 0.07)", &f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, GetNextFilterMixed2) {
	int err = 0;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);		
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();	
	Record tempRec;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", &f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}


TEST_F(SortedFileTest, FilterMatchSortAtts) {
	get_sort_order (" (l_quantity)", &f_schema, sortorder);
	startup.order = &sortorder;
	startup.runlen = runlen;
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", &f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

TEST_F(SortedFileTest, FilterNotMatchSortAtts) {	
	dbfile.Create(testFile_path[0].c_str(), sorted , &startup);	
	dbfile.Load (f_schema, tbl_dir.c_str());
	dbfile.MoveFirst();	
	Record tempRec;
	int err = 0;
	get_cnf ("(l_shipdate > '1994-01-01') AND (l_shipdate < '1994-01-07') AND (l_discount > 0.05) AND (l_discount < 0.06) AND (l_quantity > 4.0) ", &f_schema, cnf_pred, literal);
	while ( dbfile.GetNext(tempRec, cnf_pred, literal) == 1 ){
		if(comp.Compare(&tempRec, &literal, &cnf_pred) == 0){
				err++;
		}		
	}
	EXPECT_EQ( 0 , err );	
}

/*
 * RelOp unit test
 * Written by Rui 2015.03.20	
 */ 

void get_cnf (char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
	init_lexical_parser_cnf (input);
  	if (yycnfparse() != 0) {
		cout << " Error: can't parse your CNF " << input << endl;
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, right, literal); // constructs CNF predicate
	close_lexical_parser_cnf ();
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
		f_schema = nullptr;
		f_schema2 = nullptr;
		dbfile.Create(testFile_path[0].c_str(), heap , 0);
		dbfile2.Create(testFile_path[1].c_str(), heap , 0);
	}

	virtual void TearDown() {
		delete f_schema;
		delete f_schema2;
		dbfile.Close();
		dbfile2.Close();
		remove(testFile_path[0].c_str());
		remove(headFile_path[0].c_str());
		remove(testFile_path[1].c_str());
		remove(headFile_path[1].c_str());
	}
	DBFile dbfile, dbfile2;
	ComparisonEngine comp;	
	OrderMaker sortorder;
	static const int numpgs = 100;
	Schema *f_schema, *f_schema2;
public:
	void initDBFile(Table table, int flag);
	int clear_pipe(Pipe &input, CNF &cnf_pred, Record &literal);
	int check_atts_num (Pipe &input, int numAtts);
	int check_tuple_num(Pipe &input);
};

void RelOpTest::initDBFile(Table table, int flag) {	
	Schema *schema = new Schema(catalog_path.c_str(), tbl_name[(int)table] .c_str());
	string tbl_dir = tbl_prefix + tbl_name[(int)table] + ".tbl";
	if (1 == flag) {
		delete f_schema;
		dbfile.Load (*schema, tbl_dir.c_str());
		f_schema = schema;
	}else {
		delete f_schema2;
		dbfile2.Load (*schema, tbl_dir.c_str());
		f_schema2 = schema;
	}
	schema = nullptr;
}
	

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
	initDBFile(LINEITEM, 1);
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
	initDBFile(LINEITEM, 1);
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
	initDBFile(LINEITEM, 1);
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
	initDBFile(LINEITEM, 1);
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
	initDBFile(LINEITEM, 1);
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
	initDBFile(LINEITEM, 1);
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

TEST_F(RelOpTest, DuplicateRemoval1) {
	CNF sf_pred, p_pred;
	Record lit_sf, lit_p;
	SelectFile SF;
	initDBFile(LINEITEM, 1);
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

TEST_F(RelOpTest, DuplicateRemoval2) {
	CNF sf_pred, p_pred;
	Record lit_sf, lit_p;
	SelectFile SF;
	initDBFile(SUPPLIER, 1);
	get_cnf ("(s_suppkey = s_suppkey)", f_schema, sf_pred, lit_sf);
	Pipe su(100);
	SF.Run (dbfile, su, sf_pred, lit_sf);
	Project P;
		int keepMe[] = {3};
		int numAttsIn = 7;
		int numAttsOut = 1;
		Pipe psu(100);
	P.Run (su, psu, keepMe, numAttsIn, numAttsOut);
	DuplicateRemoval D;
		// inpipe = __ps
		Pipe output(100);
		Attribute IA = {"s_nationkey", Int};
		Schema out_sch ("test", 1, &IA);
	D.Run (psu, output, out_sch);
	int count = check_tuple_num(output);
	SF.WaitUntilDone();
	P.WaitUntilDone();
	D.WaitUntilDone();
	EXPECT_EQ( 25, count );
}

TEST_F(RelOpTest, Join1) {
	CNF sf1_pred, sf2_pred, j_pred;
	Record lit_sf1, lit_sf2, lit_j;
	SelectFile SF1, SF2;
	initDBFile(SUPPLIER, 1);
	get_cnf ("(s_suppkey = s_suppkey)", f_schema, sf1_pred, lit_sf1);
	Pipe su(100);
	SF1.Run (dbfile, su, sf1_pred, lit_sf1);
	initDBFile(PARTSUPP, 2);
	get_cnf ("(ps_suppkey = ps_suppkey)", f_schema2, sf2_pred, lit_sf2);
	Pipe ps(100);
	SF2.Run (dbfile2, ps, sf2_pred, lit_sf2);
	Join J;
		Pipe s_ps (100);
		get_cnf ("(s_suppkey = ps_suppkey)", f_schema, f_schema2, j_pred, lit_j);
	J.Run (su, ps, s_ps, j_pred, lit_j);
	int err = check_atts_num (s_ps, 12);
	SF1.WaitUntilDone();
	SF2.WaitUntilDone();
	J.WaitUntilDone();	
	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, Join2) {
	CNF sf1_pred, sf2_pred, j_pred, out_pred;
	Record lit_sf1, lit_sf2, lit_j, lit_out;
	SelectFile SF1, SF2;
	initDBFile(SUPPLIER, 1);
	get_cnf ("(s_suppkey = s_suppkey)", f_schema, sf1_pred, lit_sf1);
	Pipe su(100);
	SF1.Run (dbfile, su, sf1_pred, lit_sf1);
	initDBFile(PARTSUPP, 2);
	get_cnf ("(ps_suppkey = ps_suppkey)", f_schema2, sf2_pred, lit_sf2);
	Pipe ps(100);
	SF2.Run (dbfile2, ps, sf2_pred, lit_sf2);
	Join J;
		Pipe s_ps (100);
		get_cnf ("(s_suppkey = ps_suppkey)", f_schema, f_schema2, j_pred, lit_j);
		int outAtts = 12;	
		Attribute IA = {"int", Int};
		Attribute SA = {"string", String};
		Attribute DA = {"double", Double};
		Attribute s_suppkey  = {"s_suppkey", Int};
		Attribute ps_suppkey = {"ps_suppkey", Int};
		Attribute joinatt[] = {s_suppkey,SA,SA,IA,SA,DA,SA, IA,ps_suppkey,IA,DA,SA};
		Schema join_sch ("join_sch", outAtts, joinatt);
	J.Run (su, ps, s_ps, j_pred, lit_j);

	get_cnf ("(s_suppkey = ps_suppkey)", &join_sch, out_pred, lit_out);
	int err =  clear_pipe (s_ps, out_pred, lit_out);
	SF1.WaitUntilDone();
	SF2.WaitUntilDone();
	J.WaitUntilDone();	
	EXPECT_EQ( 0, err );
}


TEST_F(RelOpTest, Sum1) {
	CNF cnf_pred, out_pred;
	Record literal, lit_out;	
	SelectFile SF;	
	initDBFile(REGION, 1);
	get_cnf ("(r_regionkey = r_regionkey)", f_schema, cnf_pred, literal);
	Pipe r(10);
	SF.Run (dbfile, r, cnf_pred, literal);

	Sum T;
		// _s (input pipe)
		Pipe output (1);
		Function func;
			get_cnf ("(r_regionkey)", f_schema, func);
			Attribute sum = {"sum", Int};
			Schema sum_sch ("sum_sch", 1, &sum);
			get_cnf ("(sum = 10)", &sum_sch, out_pred, lit_out);

	T.Run (r, output, func);	
	int err =  clear_pipe (output, out_pred, lit_out);
	SF.WaitUntilDone ();
	T.WaitUntilDone();

	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, Sum2) {
	CNF cnf_pred, out_pred;
	Record literal, lit_out;	
	SelectFile SF;	
	initDBFile(SUPPLIER, 1);
	get_cnf ("(s_suppkey = s_suppkey)", f_schema, cnf_pred, literal);
	Pipe r(100);
	SF.Run (dbfile, r, cnf_pred, literal);

	Sum T;
		// _s (input pipe)
		Pipe output (1);
		Function func;
			get_cnf ("(s_acctbal)", f_schema, func);
			Attribute sum = {"sum", Double};
			Schema sum_sch ("sum_sch", 1, &sum);
			get_cnf ("(sum = 400930.0)", &sum_sch, out_pred, lit_out);

	T.Run (r, output, func);	
	int err =  clear_pipe (output, out_pred, lit_out);
	SF.WaitUntilDone ();
	T.WaitUntilDone();

	EXPECT_EQ( 0, err );
}

TEST_F(RelOpTest, GroupBy) {
	CNF cnf_pred, out_pred;
	Record literal, lit_out;	
	SelectFile SF;	
	initDBFile(SUPPLIER, 1);
	get_cnf ("(s_suppkey = s_suppkey)", f_schema, cnf_pred, literal);
	Pipe su(100);
	SF.Run (dbfile, su, cnf_pred, literal);
	GroupBy G;
		// _s (input pipe)
		Pipe output (100);
		Function func;
			get_cnf ("(s_acctbal)", f_schema, func);
			OrderMaker grp_order;
			grp_order.Add(3, Int);

	G.Run(su, output, grp_order, func);
	int count = check_tuple_num(output);	
	SF.WaitUntilDone ();
	G.WaitUntilDone();
	EXPECT_EQ( 25, count );
}

TEST_F(RelOpTest, WriteOut) {
	CNF cnf_pred;
	Record literal, rec;	
	SelectFile SF;	
	initDBFile(CUSTOMER, 1);
	get_cnf ("(c_custkey = c_custkey)", f_schema, cnf_pred, literal);
	Pipe cu(100);

	WriteOut W;
		char *fwpath = "test.w.tmp";
		FILE *writefile = fopen (fwpath, "w");
		
	ASSERT_TRUE(writefile != NULL);
	SF.Run (dbfile, cu, cnf_pred, literal);
	W.Run (cu, writefile, *f_schema, OUTFILE_);
	SF.WaitUntilDone ();
	W.WaitUntilDone();
	
	writefile = fopen (fwpath, "r");
	int count = 0;
	while (rec.SuckNextRecord(f_schema, writefile)) {
		count++;
	}
	EXPECT_EQ( 1500, count );
}

/*
 * Statistics unit test
 * Written by Rui 2015.04.13	
 */ 
string StatFileName = "dbfile/Statistics.txt";
class StatisticsTest : public ::testing::Test {
protected:
	Statistics s;
};

TEST_F(StatisticsTest, EqualSeletion) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "R.a",10);
	char *cnf = "(R.a = 10)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(1000, result, 1);
}

TEST_F(StatisticsTest, NonEqualSeletion) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],30000);
	s.AddAtt(relName[0], "R.a",UNKNOWN);
	char *cnf = "(R.a > 10)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(10000, result, 1);
}

TEST_F(StatisticsTest, DependentOR) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],30000);
	s.AddAtt(relName[0], "R.a", 30);
	char *cnf = "(R.a = 10 OR R.a = 20)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(2000, result, 1);
}

TEST_F(StatisticsTest, IndependentOR) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],30000);
	s.AddAtt(relName[0], "R.a",UNKNOWN);
	s.AddAtt(relName[0], "R.b",10);
	char *cnf = "(R.a > 10 OR R.b = 5)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(12000, result, 1);
}

TEST_F(StatisticsTest, SelectionAND) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],30000);
	s.AddAtt(relName[0], "R.a",UNKNOWN);
	s.AddAtt(relName[0], "R.b",10);
	char *cnf = "(R.a > 10) AND (R.b = 5)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(1000, result, 1);
}

TEST_F(StatisticsTest, SelectionANDwithOrList) {
	char *relName[] = {"R"};	
	s.AddRel(relName[0],30000);
	s.AddAtt(relName[0], "R.a",UNKNOWN);
	s.AddAtt(relName[0], "R.b",3);
	s.AddAtt(relName[0], "R.c",10);
	char *cnf = "(R.a > 10) AND (R.b = 5 OR R.c = 10)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 1);
	EXPECT_NEAR(4000, result, 1);
}

TEST_F(StatisticsTest, EqualJoin) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",1000);
	s.AddAtt(relName[1], "S.b",5000);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(600000, result, 1);
}

TEST_F(StatisticsTest, NonEqualJoin) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],3000);
	s.AddRel(relName[1],10000);
	s.AddAtt(relName[0], "R.a",UNKNOWN);
	s.AddAtt(relName[1], "S.b",UNKNOWN);
	char *cnf = "(R.a > S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(10000000, result, 1);
}

TEST_F(StatisticsTest, EqualJoinThreeRelation) {
	char *relName[] = {"R", "S", "T"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddRel(relName[2],20000);
	s.AddAtt(relName[0], "R.a",1000);
	s.AddAtt(relName[1], "S.b",5000);
	s.AddAtt(relName[2], "T.c",10000);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 2);
	cnf = "(R.a = T.c)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 3);
	EXPECT_NEAR(1200000, result, 1);
}

TEST_F(StatisticsTest, EqualJoinFourRelation) {
	char *relName[] = {"R", "S", "T", "U"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddRel(relName[2],20000);
	s.AddRel(relName[3],50000);
	s.AddAtt(relName[0], "R.a",1000);
	s.AddAtt(relName[1], "S.b",5000);
	s.AddAtt(relName[2], "T.c",10000);
	s.AddAtt(relName[3], "U.d",10000);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 2);
	cnf = "(R.a = T.c)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 3);
	cnf = "(U.d = T.c)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 4);
	EXPECT_NEAR(6000000, result, 1);
}

TEST_F(StatisticsTest, EqualJoinAndSelect) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",1000);
	s.AddAtt(relName[1], "S.b",5000);
	char *cnf = "(R.a = S.b) AND (R.a = 2)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(600, result, 1);
}

TEST_F(StatisticsTest, EqualJoinAndSelectOrList) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",100);
	s.AddAtt(relName[1], "S.b",300);
	char *cnf = "(R.a = S.b) AND (R.a = 2 OR S.b = 100)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(133000, result, 1);
}

TEST_F(StatisticsTest, EqualJoinAndSelectAndList) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",100);
	s.AddAtt(relName[1], "S.b",200);
	char *cnf = "(R.a = S.b) AND (R.a = 2) AND (S.b = 100)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(750, result, 1);
}

TEST_F(StatisticsTest, EqualJoinAfterApplySelect) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",100);
	s.AddAtt(relName[1], "S.b",200);
	char *cnf = "(R.a = 2)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 1);
	cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(150000, result, 1);
}

TEST_F(StatisticsTest, SelectAfterApplyEqualJoin) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",100);
	s.AddAtt(relName[1], "S.b",200);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 2);
	cnf = "(R.a = 2)";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, relName, 2);
	EXPECT_NEAR(150000, result, 1);
}

TEST_F(StatisticsTest, ReadAndWrite1) {
	char *relName[] = {"R", "S"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddAtt(relName[0], "R.a",100);
	s.AddAtt(relName[1], "S.b",200);
	char *cnf = "(R.a = 2)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 1);
	s.Write(StatFileName.c_str());
	cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	Statistics s1;
	s1.Read(StatFileName.c_str());
	double result = s1.Estimate(final, relName, 2);
	EXPECT_NEAR(150000, result, 1);
	remove(StatFileName.c_str());
}

TEST_F(StatisticsTest, ReadAndWrite2) {
	char *relName[] = {"R", "S", "T"};	
	s.AddRel(relName[0],30000);
	s.AddRel(relName[1],100000);
	s.AddRel(relName[2],20000);
	s.AddAtt(relName[0], "R.a",1000);
	s.AddAtt(relName[1], "S.b",5000);
	s.AddAtt(relName[2], "T.c",10000);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 2);
	s.Write(StatFileName.c_str());
	cnf = "(R.a = T.c)";
	yycnf_scan_string(cnf);
	yycnfparse();
	Statistics s1;
	s1.Read(StatFileName.c_str());
	double result = s1.Estimate(final, relName, 3);
	EXPECT_NEAR(1200000, result, 1);	
	remove(StatFileName.c_str());
}

TEST_F(StatisticsTest, CopyRelation) {
	char *relName[] = {"R","S","T"};
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "a",25);
	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "b",150000);
	s.AddAtt(relName[1], "c",25);
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "d",25);

	s.CopyRel("T","t1");
	s.CopyRel("T","t2");
	s.CopyRel("R","r");
	s.CopyRel("S","s");

	char *set1[] ={"r","t1"};
	char *cnf = "(r.a = t1.d)";
	yycnf_scan_string(cnf);
	yycnfparse();	
	s.Apply(final, set1, 2);
	
	char *set2[] ={"s","t2"};
	cnf = "(s.c = t2.d)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, set2, 2);

	char *set3[] = {"s","r","t1","t2"};
	cnf = " (t1.d = t2.d )";
	yycnf_scan_string(cnf);
	yycnfparse();
	double result = s.Estimate(final, set3, 4);
	EXPECT_NEAR(60000000, result, 1);
}


TEST(StatisticsDeathTest, AddNumDistinctLargerThanNumTuples) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	 char *relName[] = {"R"};	
	s.AddRel(relName[0],10);	
	EXPECT_DEATH( s.AddAtt(relName[0], "R.a", 11), "ERROR: Attempt to add distinct value greater than number of tuples!");
}

TEST(StatisticsDeathTest, AddNumDistinctToNonExistRelation) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R"};	
	s.AddRel(relName[0],10);	
	EXPECT_DEATH( s.AddAtt("S", "R.a", 1), "ERROR: Attempt to add attribute to non-exist relation!");
}

TEST(StatisticsDeathTest, AddNullRelation) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	
	EXPECT_DEATH( s.AddRel(nullptr, 10), "ERROR: Invalid relation name in AddRel!");
}

TEST(StatisticsDeathTest, AddNullAttribute) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	 char *relName[] = {"R"};	
	s.AddRel(relName[0],10);	
	EXPECT_DEATH( s.AddAtt("R", nullptr, 1), "ERROR: Invalid relation name or attribute name in AddAtt!");
}

TEST(StatisticsDeathTest,SelectNonExistAttribute) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R"};	
	s.AddRel(relName[0],1000);
	char *cnf = "(R.a = 1)";
	yycnf_scan_string(cnf);
	yycnfparse();
	EXPECT_DEATH( s.Estimate(final, relName, 1), "ERROR: Attempt to select on non-exist attribute! R.a");
}

TEST(StatisticsDeathTest, JoinNonExistAttribute) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R", "S"};
	s.AddRel(relName[0],100);
	s.AddRel(relName[1],200);
	s.AddAtt(relName[1], "S.b",10);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	EXPECT_DEATH( s.Estimate(final, relName, 2), "ERROR: Attempt to join on non-exist attribute! R.a=S.b");
}

TEST(StatisticsDeathTest, SelectNonExistRelation) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R"};
	char *wrongName[] = {"S"};
	s.AddRel(relName[0],100);
	s.AddAtt(relName[0], "R.a",10);
	char *cnf = "(R.a = 1)";
	yycnf_scan_string(cnf);
	yycnfparse();
	EXPECT_DEATH( s.Estimate(final, wrongName, 1), "ERROR: Current statistics does not contain S");
}

TEST(StatisticsDeathTest, JoinNonExistRelation) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R", "S"};
	char *wrongName[] = {"R", "T"};
	s.AddRel(relName[0],100);
	s.AddRel(relName[1],200);
	s.AddAtt(relName[1], "S.b",10);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	EXPECT_DEATH( s.Estimate(final, wrongName, 2), "ERROR: Current statistics does not contain T");
}

TEST(StatisticsDeathTest, JoinIncompleteSubset) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	Statistics s;
	char *relName[] = {"R", "S"};
	s.AddRel(relName[0],100);
	s.AddRel(relName[1],100);	
	s.AddAtt(relName[0], "R.a",10);
	s.AddAtt(relName[1], "S.b",10);
	char *cnf = "(R.a = S.b)";
	yycnf_scan_string(cnf);
	yycnfparse();
	s.Apply(final, relName, 2);
	s.Write(StatFileName.c_str());
	Statistics s1;
	s1.Read(StatFileName.c_str());
	char *relName2[] = {"T", "R"};
	s1.AddRel(relName2[0],100);
	s1.AddAtt(relName2[0], "T.c",10);
	cnf = "(R.a = T.c)";
	yycnf_scan_string(cnf);
	yycnfparse();
	EXPECT_DEATH( s1.Estimate(final, relName2, 2), "ERROR: Attempt to join incomplete subset members");
}


GTEST_API_ int main(int argc, char **argv) {
	printf("Running main() from gtest_main.cc\n");
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}

