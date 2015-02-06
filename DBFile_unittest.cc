#include <limits.h>
#include "DBFile.h"
#include "Schema.h"
#include "Record.h"
#include "gtest/gtest.h"
#include "stdio.h"

char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl"; 
char tbl_name[50] = "lineitem";
char catalog_path[100] = "catalog";
char testFile_path[256] = "dbfile/testFile.bin";


class DBFileTest : public ::testing::Test {
	protected:
	virtual void SetUp() {
		dbfile.Create(testFile_path, heap , 0);
	}

	virtual void TearDown() {
		dbfile.Close();
	}
	DBFile dbfile;		
};

TEST_F(DBFileTest, CreateFile) {
	FILE * fp = fopen(testFile_path,"r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(DBFileTest, OpenFile) {
	int f_flag = dbfile.Open(testFile_path);
	EXPECT_EQ( 1 , f_flag );	
}

TEST_F(DBFileTest, CloseFile) {
 	EXPECT_EQ(1 , dbfile.Close() );     //close created file
	dbfile.Open(testFile_path); 
	EXPECT_EQ(1 , dbfile.Close() );		//close open file
}

TEST_F(DBFileTest, CreateHeapTypeHeader) {
	char type[8];
	FILE * fp = fopen("dbfile/testFile.header","r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
	EXPECT_STREQ("heap", type);	
}

TEST_F(DBFileTest, LoadFile) {
	Schema f_schema(catalog_path, tbl_name);
	EXPECT_EQ( 0, dbfile.curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir);
	EXPECT_LE( 2, dbfile.curFile.GetLength() );
}

TEST_F(DBFileTest, MoveFirst) {
	EXPECT_EQ( 0, dbfile.curPageIndex );
}

TEST_F(DBFileTest, GetNextRecord) {
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

TEST_F(DBFileTest, AddRecord) {
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


TEST(DBFileDeathTest, OpenNonExistFile) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Open("dbfile/nonexist.bin"), "BAD!  Open did not work for dbfile/nonexist.bin");	
}

TEST(DBFileDeathTest, CreateInNonExistDir) {
	::testing::FLAGS_gtest_death_test_style = "threadsafe";
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Create("nonexist/testFile.bin", heap , 0), "BAD!  Open did not work for nonexist/testFile.bin");
}

TEST(DBFileDeathTest, AddRecordLTPage) {
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






