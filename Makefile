tag = -i
ifdef linux
tag = -n
endif

GTEST_DIR = ./source/gtest
USER_DIR = ./source
BIN_DIR = ./bin
CPPFLAGS += -isystem $(GTEST_DIR)/include
CXXFLAGS += -std=c++11 -g -O2 -pthread -Wno-write-strings -Wno-unused-result # -Wall -Wextra
TESTS = Database_unittest 
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc # $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

main: main.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o PlanTree.o ParseInfo.o Tables.o
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/main  main.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o PlanTree.o ParseInfo.o Tables.o -lfl

a5test.out: a5test.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o PlanTree.o ParseInfo.o Tables.o
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/a5test.out a5test.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Statistics.o Function.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o PlanTree.o ParseInfo.o Tables.o -lfl

main.o: $(USER_DIR)/main.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/main.cc

a5test.o : $(USER_DIR)/a5test.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -g -c $(USER_DIR)/a5test.cc

Database_unittest: Database_unittest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o GenericDBFile.o HeapDBFile.o SortedDBFile.o Pipe.o BigQ.o RelOp.o Function.o Statistics.o PlanTree.o ParseInfo.o Tables.o y.tab.o yyfunc.tab.o yycnf.tab.o  lex.yy.o lex.yyfunc.o lex.yycnf.o gtest_main.a
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $(BIN_DIR)/$@

Database_unittest.o : $(USER_DIR)/Database_unittest.cc \
                     $(USER_DIR)/*.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Database_unittest.cc

DBFile.o : $(USER_DIR)/DBFile.cc $(USER_DIR)/DBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/DBFile.cc

GenericDBFile.o : $(USER_DIR)/GenericDBFile.cc $(USER_DIR)/GenericDBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/GenericDBFile.cc

SortedDBFile.o : $(USER_DIR)/SortedDBFile.cc $(USER_DIR)/SortedDBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/SortedDBFile.cc

HeapDBFile.o : $(USER_DIR)/HeapDBFile.cc $(USER_DIR)/HeapDBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/HeapDBFile.cc

ParseInfo.o: $(USER_DIR)/ParseInfo.cc $(USER_DIR)/ParseInfo.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/ParseInfo.cc

Tables.o: $(USER_DIR)/Tables.cc $(USER_DIR)/Tables.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Tables.cc

PlanTree.o : $(USER_DIR)/PlanTree.cc $(USER_DIR)/PlanTree.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/PlanTree.cc

Statistics.o : $(USER_DIR)/Statistics.cc $(USER_DIR)/Statistics.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Statistics.cc

RelOp.o : $(USER_DIR)/RelOp.cc $(USER_DIR)/RelOp.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/RelOp.cc

Function.o : $(USER_DIR)/Function.cc $(USER_DIR)/Function.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Function.cc

Pipe.o: $(USER_DIR)/Pipe.cc $(USER_DIR)/Pipe.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Pipe.cc

BigQ.o: $(USER_DIR)/BigQ.cc $(USER_DIR)/BigQ.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/BigQ.cc

Comparison.o : $(USER_DIR)/Comparison.cc $(USER_DIR)/Comparison.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Comparison.cc

ComparisonEngine.o : $(USER_DIR)/ComparisonEngine.cc $(USER_DIR)/ComparisonEngine.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/ComparisonEngine.cc

File.o : $(USER_DIR)/File.cc $(USER_DIR)/File.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/File.cc

Record.o : $(USER_DIR)/Record.cc $(USER_DIR)/Record.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Record.cc

Schema.o : $(USER_DIR)/Schema.cc $(USER_DIR)/Schema.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/Schema.cc

y.tab.o: $(USER_DIR)/Parser.y
	yacc -d $(USER_DIR)/Parser.y --output=$(USER_DIR)/y.tab.c
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -Wno-write-strings -c $(USER_DIR)/y.tab.c -o y.tab.o

yyfunc.tab.o: $(USER_DIR)/ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d $(USER_DIR)/ParserFunc.y --output=$(USER_DIR)/yyfunc.tab.c
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -Wno-write-strings -c $(USER_DIR)/yyfunc.tab.c -o yyfunc.tab.o

yycnf.tab.o: $(USER_DIR)/ParserCNF.y
	yacc -p "yycnf" -b "yycnf" -d $(USER_DIR)/ParserCNF.y --output=$(USER_DIR)/yycnf.tab.c
	g++ -Wno-write-strings -c $(USER_DIR)/yycnf.tab.c -o yycnf.tab.o

lex.yy.o: $(USER_DIR)/Lexer.l
	lex --outfile=$(USER_DIR)/lex.yy.c $(USER_DIR)/Lexer.l
	gcc -Wno-write-strings -c $(USER_DIR)/lex.yy.c -o lex.yy.o

lex.yyfunc.o: $(USER_DIR)/LexerFunc.l
	lex -Pyyfunc --outfile=$(USER_DIR)/lex.yyfunc.c  $(USER_DIR)/LexerFunc.l 
	gcc -Wno-write-strings -c $(USER_DIR)/lex.yyfunc.c -o lex.yyfunc.o

lex.yycnf.o: $(USER_DIR)/LexerCNF.l
	lex -Pyycnf --outfile=$(USER_DIR)/lex.yycnf.c  $(USER_DIR)/LexerCNF.l 
	gcc -Wno-write-strings -c $(USER_DIR)/lex.yycnf.c -o lex.yycnf.o

gtest_main.a : gtest-all.o 
	$(AR) $(ARFLAGS) $@ $^

gtest.a : $(OBJ_DIR)/gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc 

test: Database_unittest

clean: 
	rm -f *.o
	rm -f $(USER_DIR)/y.tab.c
	rm -f $(USER_DIR)/y.tab.h
	rm -f $(USER_DIR)/lex.yy.c
	rm -f $(USER_DIR)/yyfunc.tab.c
	rm -f $(USER_DIR)/yyfunc.tab.h
	rm -f $(USER_DIR)/lex.yyfunc.c
	rm -f $(USER_DIR)/yycnf.tab.c
	rm -f $(USER_DIR)/yycnf.tab.h
	rm -f $(USER_DIR)/lex.yycnf.c
#	rm -f gtest.a gtest_main.a
	rm -f $(BIN_DIR)/$(TESTS)
#	rm -f $(BIN_DIR)/dbfile/*.bin 
#	rm -f $(BIN_DIR)/dbfile/*.header
	rm -f $(BIN_DIR)/dbfile/*.bigq
	rm -f $(BIN_DIR)/dbfile/temp/*
	rm -f $(BIN_DIR)/dbfile/test/*
	rm -f $(USER_DIR)/*~










