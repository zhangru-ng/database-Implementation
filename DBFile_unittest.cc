#include <limits.h>
#include "DBFile.h"
#include "gtest/gtest.h"
#include "stdio.h"

class DBFileTest : public ::testing::Test {
	protected:
	virtual void SetUp() {
		dbfile.Create("testFile.bin",heap,0);
		f_schema = ("source/catalog", "lineitem");
		strcpy(tbl_dir, "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl");
	}

	virtual void TearDown() {
		dbfile.Close();
	}
	DBFile dbfile;	
	Schema f_schema;
	char tbl_dir[256]; 
	Record tempRec1, tempRec2;
};

TEST_F(DBFileTest, CreateFile) {
	FILE * fp = fopen("testFile.bin","r");
	ASSERT_TRUE(fp != NULL);	
	fclose(fp);	
}

TEST_F(DBFileTest, OpenFile) {
	int f_flag = dbfile.Open("testFile.bin");
	EXPECT_EQ( 1 , f_flag );	
}

TEST_F(DBFileTest, CloseFile) {
	int f_flag = dbfile.Close();
	EXPECT_EQ(1 , f_flag);   //close created file
	dbfile.Open("testFile.bin");
	f_flag = dbfile.Close(); //close open file
	EXPECT_EQ(1 , f_flag);	
	dbfile.Create("testFile.bin",heap,0);	//avoid close twice, may be not necessary
}

TEST_F(DBFileTest, CreateHeapTypeHeader) {
	char type[8];
	FILE * fp = fopen("testFile.bin.header","r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
	EXPECT_STREQ("heap", type);	
}

TEST_F(DBFileTest, LoadFile) {
	EXPECT_EQ( 0, dbfile.curFile.GetLength() );
	dbfile.Load (f_schema, tbl_dir);
	EXPECT_GE( 2, dbfile.curFile.GetLength() );
}

TEST_F(DBFileTest, MoveFirst) {
	EXPECT_EQ( 0, dbfile.curPageIndex );
}

TEST_F(DBFileTest, AddRecord) {
	int i;
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	
	EXPECT_EQ( 0, dbfile.curFile.GetLength() );
	
	tempRec1.SuckNextRecord (&f_schema, tableFile);	
	dbfile.Add (tempRec1);
	EXPECT_EQ( 2, dbfile.curFile.GetLength() );
	
	for(i = 0; i<800; i++){
		tempRec1.SuckNextRecord (&f_schema, tableFile);	
		dbfile.Add (tempRec);
	}
	EXPECT_EQ( 3, dbfile.curFile.GetLength() );	
	fclose(tableFile);
}

TEST_F(DBFileTest, GetNext) {
	int i;
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	
	while ( dbfile.GetNext(tempRec2) && tempRec1.SuckNextRecord (&f_schema, tableFile) ){
		EXPECT_EQ( 0, strcmp( tempRec1.bits, tempRec2.bits );
	}
	fclose(tableFile);
}

TEST(DBFileDeathTest, OpenNonExistFile) {
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Open("nonexist.bin"), "BAD!  Open did not work for nonexist.bin");	
}





