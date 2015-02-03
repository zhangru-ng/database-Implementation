CC = g++ -O2 -Wno-deprecated 
tag = -i

ifdef linux
tag = -n
endif

GTEST_DIR = ./gtest
USER_DIR = .
CPPFLAGS += -isystem $(GTEST_DIR)/include
CXXFLAGS += -g -pthread -O2 -Wno-deprecated 
TESTS = DBFile_unittest 
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)


test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o -lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl

test.o: test.cc
	$(CC) -g -c test.cc

main.o: main.cc
	$(CC) -g -c main.cc

DBFile_unittest: DBFile.o DBFile_unittest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o gtest_main.a
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $@

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
	sed $(tag) $(USER_DIR)/y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c $(USER_DIR)/y.tab.c

lex.yy.o: $(USER_DIR)/Lexer.l
	lex  $(USER_DIR)/Lexer.l
	gcc  -c $(USER_DIR)/lex.yy.c

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc


out: test.out main
testcase: DBFile_unittest

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f main
	rm -f $(TESTS) gtest.a gtest_main.a 











