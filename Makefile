CC = g++ -O2 -Wno-write-strings
tag = -i

ifdef linux
tag = -n
endif

GTEST_DIR = ./source/gtest
USER_DIR = ./source
BIN_DIR = ./bin
CPPFLAGS += -isystem $(GTEST_DIR)/include
CXXFLAGS += -g -pthread -Wall -Wextra -Wno-write-strings
TESTS = DBFile_unittest 
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)


test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o a1test.o
	$(CC) -o  $(BIN_DIR)/test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o a1test.o -lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o $(BIN_DIR)/main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl

a1test.o : $(USER_DIR)/a1test.cc $(USER_DIR)/a1test.h $(GTEST_HEADERS)
	$(CC) $(CPPFLAGS) -c $(USER_DIR)/a1test.cc

main.o: $(USER_DIR)/main.cc
	$(CC) $(CPPFLAGS) -c $(USER_DIR)/main.cc

DBFile_unittest: DBFile.o DBFile_unittest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o gtest_main.a
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $(BIN_DIR)/$@
#	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $@

DBFile_unittest.o : $(USER_DIR)/DBFile_unittest.cc \
                     $(USER_DIR)/*.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/DBFile_unittest.cc

DBFile.o : $(USER_DIR)/DBFile.cc $(USER_DIR)/DBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/DBFile.cc

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
	yacc -d $(USER_DIR)/Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -Wno-write-strings -c y.tab.c -o y.tab.o

lex.yy.o: $(USER_DIR)/Lexer.l
	lex $(USER_DIR)/Lexer.l
	gcc -Wno-write-strings -c lex.yy.c -o lex.yy.o

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

gtest.a : $(OBJ_DIR)/gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc 

gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc 

all:  test.out main DBFile_unittest
out:  test.out main
test: DBFile_unittest

clean: 
	rm -f *.o
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f gtest.a gtest_main.a
	rm -f $(BIN_DIR)/main $(BIN_DIR)/test.out
	rm -f $(BIN_DIR)/$(TESTS)
	rm -f $(BIN_DIR)/dbfile/*.bin 
	rm -f $(BIN_DIR)/dbfile/*.header









