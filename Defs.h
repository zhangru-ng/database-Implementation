#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 40
#define MAX_ORS 20

#define PAGE_SIZE 131072


enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int = 1, Double, String};

typedef enum {heap, sorted, tree} fType;
//the current mode of the file: Read or Write
typedef enum _FileMode{ Read, Write } FileMode;

unsigned int Random_Generate();


#endif

