/***********************************************************************
 * SECTION 1
 ***********************************************************************/
/* The code in %{ %} is included as it is in lex.yycnf.c file
 * it has C global variables, prototypes, and comments 
 */


%{

#include <string.h> // For strdup()
#include <stdlib.h> // For malloc()
#include "ParseTree.h"
#include "yycnf.tab.h"

//int yycnflineno = 1;
void yycnferror(char*s);

static YY_BUFFER_STATE yycnf_buf_state;
void init_lexical_parser_cnf (char *src) { yycnf_buf_state = yycnf_scan_string (src); }
void close_lexical_parser_cnf () { yycnf_delete_buffer (yycnf_buf_state); }
%}


/******************************************************************************
 * SECTION 2
 ******************************************************************************/
/* This is the DEFINITION section which contains substitutions, code, and
 * start stats; will be copied into lex.yy.c
 */

/******************************************************************************
 * SECTION 3
 ******************************************************************************/
/* This is the RULES section which defines how to "scan" and what action
 * to take for each token
 */

%%

"("			return('(');

")"    	        	return(')');

"<"    		      	return('<');

">"    		 	return('>');

"=" 			return('=');

-?[0-9]+ 	       {yycnflval.actualChars = strdup(yycnftext);
  			return(Int); 
		        }

-?[0-9]+\.[0-9]*       {yycnflval.actualChars = strdup(yycnftext); 
  			return(Float);
			}

"OR"      		return(OR);

"AND"			return(AND);

\'[^'\n]*\' 	       {/* take care of ' in a string */
	 		if (yycnftext[yycnfleng - 2] == '\\') {
     				yymore();
   			} else {
				yycnflval.actualChars = strdup(yycnftext + 1);	
				yycnflval.actualChars[strlen(yycnflval.actualChars) - 1] = 0;
     				return(String);
   			}
			}


[A-Za-z][\.A-Za-z0-9_-]* {yycnflval.actualChars = strdup(yycnftext);
  			return(Name);
			}     

\n                	yycnflineno++;

[ \t]             	;

.                 	yycnferror("LEX_ERROR: invalid character");

%%

void yycnferror(char *s) {
  printf("%d: %s at %s\n", yycnflineno, s, yycnftext);
}

int yycnfwrap(void){
  return 1;
}


