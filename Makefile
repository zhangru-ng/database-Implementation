CC = g++ -O2 -Wno-deprecated 
tag = -i

ifdef linux
tag = -n
endif

GTEST_DIR = ./source/gtest
USER_DIR = ./source
BIN_DIR = ./bin
CPPFLAGS += -isystem $(GTEST_DIR)/include
CXXFLAGS += -g -pthread -Wall -Wextra -O2 -Wno-deprecated 
TESTS = DBFile_unittest 
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)


test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test0.o
	$(CC) -o  $(BIN_DIR)/test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test0.o -lfl

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl

test0.o: $(USER_DIR)/test.cc
	$(CC) -g -c $(USER_DIR)/test.cc -o test0.o

main.o: $(USER_DIR)/main.cc
	$(CC) -g -c $(USER_DIR)/main.cc -o main.o

DBFile_unittest: DBFile.o DBFile_unittest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o gtest_main.a
#	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $(BIN_DIR)/$@
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $@

DBFile_unittest.o : $(USER_DIR)/DBFile_unittest.cc \
                     $(USER_DIR)/*.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/DBFile_unittest.cc

DBFile.o : $(USER_DIR)/DBFile.cc $(USER_DIR)/DBFile.h $(GTEST_HEADERS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $(USER_DIR)/DBFile.cc

Comparison.o : $(USER_DIR)/Comparison.cc 
	$(CC) -g  -c $(USER_DIR)/Comparison.cc

ComparisonEngine.o : $(USER_DIR)/ComparisonEngine.cc 
	$(CC) -g  -c $(USER_DIR)/ComparisonEngine.cc

File.o : $(USER_DIR)/File.cc 
	$(CC) -g -c $(USER_DIR)/File.cc

Record.o : $(USER_DIR)/Record.cc 
	$(CC) -g -c $(USER_DIR)/Record.cc

Schema.o : $(USER_DIR)/Schema.cc 
	$(CC) -g -c $(USER_DIR)/Schema.cc

y.tab.o: $(USER_DIR)/Parser.y
	yacc -d $(USER_DIR)/Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c -o y.tab.o

lex.yy.o: $(USER_DIR)/Lexer.l
	lex $(USER_DIR)/Lexer.l
	gcc  -c lex.yy.c -o lex.yy.o

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

out:  test.out main
test: DBFile_unittest

clean: 
	rm -f *.o
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
	rm -f $(BIN_DIR)/main test.out
	rm -f gtest.a gtest_main.a
	rm -f $(BIN_DIR)/$(TESTS)











