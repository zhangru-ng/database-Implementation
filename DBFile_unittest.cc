#include <limits.h>
#include "DBFile.h"
#include "gtest/gtest.h"
#include "stdio.h"

TEST(DBFileTest, CreateFile) {
	DBFile dbfile;
	FILE * fp;

	dbfile.Create("testFile.bin",heap,0);
	fp = fopen("testFile.bin","r");
	ASSERT_TRUE(fp != NULL);
	dbfile.Close();
}

TEST(DBFileTest, OpenFile) {
	DBFile dbfile;
	int f_flag = 0;
	
	dbfile.Create("testFile.bin",heap,0);
	dbfile.Close();
	
	f_flag = dbfile.Open("testFile.bin");
	EXPECT_EQ(1 , f_flag);	
	
	dbfile.Close();
}

TEST(DBFileDeathTest, OpenNonExistFile) {
	DBFile dbfile;
	EXPECT_DEATH(dbfile.Open("nonexist.bin"), "BAD!  Open did not work for nonexist.bin");	
}

/*
TEST(DBFileTest, CloseNotOpenFile) {
	DBFile dbfile;
	int f_flag = 1;
	
	f_flag = dbfile.Close();
	EXPECT_EQ(0, f_flag) << f_flag;	
}*/


TEST(DBFileTest, CloseCreatedFile) {
	DBFile dbfile;
	int f_flag = 0;
	
	dbfile.Create("testFile.bin",heap,0);
	f_flag = dbfile.Close();
	EXPECT_EQ(1 , f_flag);		
	
}

TEST(DBFileTest, CloseOpenFile) {
	DBFile dbfile;
	int f_flag = 0;
	
	dbfile.Create("testFile.bin",heap,0);
	dbfile.Close();
	dbfile.Open("testFile.bin");
	f_flag = dbfile.Close();
	EXPECT_EQ(1 , f_flag);			
}


TEST(DBFileTest, CreateHeapTypeHeader) {
	DBFile dbfile;
	FILE * fp;
	char type[8];
		
	dbfile.Create("testFile.bin",heap,0);
	
	fp = fopen("testFile.bin.header","r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
	
	dbfile.Close();
	
	EXPECT_STREQ("heap", type);	
}

TEST(DBFileTest, CreateSortedTypeHeader) {
	DBFile dbfile;
	FILE * fp;
	char type[8];
	
	dbfile.Create("testFile.bin",sorted,0);
	
	fp = fopen("testFile.bin.header","r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
	
	dbfile.Close();
	
	EXPECT_STREQ("sorted", type);	
}

TEST(DBFileTest, CreateTreeTypeHeader) {
	DBFile dbfile;
	FILE * fp;
	char type[8];

	dbfile.Create("testFile.bin",tree,0);
	
	fp = fopen("testFile.bin.header","r");
	ASSERT_TRUE(fp != NULL);
	fscanf(fp,"%s",type);
	fclose(fp);		
		
	dbfile.Close();

	EXPECT_STREQ("tree", type);	
}

TEST(DBFileTest, LoadFile) {
	DBFile dbfile;
	Schema f_schema("source/catalog", "nation");
	char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/nation.tbl";
	
	dbfile.Create("testFile.bin",heap,0);
	EXPECT_EQ(dbfile.GetLength(), 0);
	dbfile.Load (f_schema, tbl_dir);
	EXPECT_GE(dbfile.GetLength(), 2);	
	dbfile.Close();
}

TEST(DBFileTest, AddRecord) {
	DBFile dbfile;
	Record tempRec;
	int i;
	char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl";
	Schema f_schema("source/catalog", "lineitem");	
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	
	
	dbfile.Create("testFile.bin",heap,0);
	EXPECT_EQ(dbfile.GetLength(), 0);
	
	tempRec.SuckNextRecord (&f_schema, tableFile);	
	dbfile.Add (tempRec);
	EXPECT_EQ(dbfile.GetLength(), 2);	

	tempRec.SuckNextRecord (&f_schema, tableFile);	
	dbfile.Add (tempRec);
	EXPECT_EQ(dbfile.GetLength(), 2);
	
	for(i = 0; i<700; i++){
		tempRec.SuckNextRecord (&f_schema, tableFile);	
		dbfile.Add (tempRec);
	}
	EXPECT_EQ(dbfile.GetLength(), 3);
	
	for(i = 0; i<700; i++){
		tempRec.SuckNextRecord (&f_schema, tableFile);	
		dbfile.Add (tempRec);
	}
	EXPECT_EQ(dbfile.GetLength(), 4);
		
	dbfile.Close();
}
	
TEST(DBFileTest, GetNext) {
	DBFile dbfile;
	Record tempRec1, tempRec2;
	int i;
	char tbl_dir[256] = "/cise/tmp/dbi_sp11/DATA/10M/lineitem.tbl";
	Schema f_schema("source/catalog", "lineitem");	
	FILE *tableFile = fopen (tbl_dir, "r");
	ASSERT_TRUE(tableFile != NULL);
	tempRec1.SuckNextRecord (&f_schema, tableFile);	
	
	
	dbfile.Create("testFile.bin",heap,0);
	dbfile.Load (f_schema, tbl_dir);
	dbfile.MoveFirst();
	for(i = 0; i<700; i++){
		EXPECT_EQ( 1, dbfile.GetNext(tempRec2) );
		tempRec1.SuckNextRecord (&f_schema, tableFile);	
	}	
	
	tempRec1.Print(&f_schema);
	cout << "\n" << endl;
	tempRec2.Print(&f_schema);
	
	for(i = 0; i<7000; i++){
		EXPECT_EQ( 1, dbfile.GetNext(tempRec2) );
		tempRec1.SuckNextRecord (&f_schema, tableFile);	
	}	
	
	tempRec1.Print(&f_schema);
	cout << "\n" << endl;
	tempRec2.Print(&f_schema);
	
	
	dbfile.Close();
}

