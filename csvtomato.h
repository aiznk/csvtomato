#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>

/************
* constants *
************/

enum {
	CSVTMT_ERR_MSG_SIZE = 256,
	CSVTMT_STR_SIZE = 256,
};

typedef enum {
	CSVTMT_ERR_NONE,
	CSVTMT_ERR_MEM,
	CSVTMT_ERR_BUF_OVERFLOW,
	CSVTMT_ERR_TOKENIZE,
} CsvTomatoErrorKind;

typedef enum {
	CSVTMT_TK_ROOT,

	CSVTMT_TK_IDENT, // indent
	CSVTMT_TK_SEMICOLON, // ;
	CSVTMT_TK_ASSIGN, // =
	CSVTMT_TK_BEG_PAREN, // (
	CSVTMT_TK_END_PAREN, // )
	CSVTMT_TK_INT,  // 123
	CSVTMT_TK_FLOAT,  // 3.14
	CSVTMT_TK_STRING,

	// reserved idents
	CSVTMT_TK_CREATE,
	CSVTMT_TK_SELECT,
	CSVTMT_TK_INSERT,
	CSVTMT_TK_INTO,
	CSVTMT_TK_VALUES,
	CSVTMT_TK_TABLE,
	CSVTMT_TK_IF,
	CSVTMT_TK_NOT,
	CSVTMT_TK_EXISTS,
	CSVTMT_TK_INTEGER,
	CSVTMT_TK_PRIMARY,
	CSVTMT_TK_KEY,
	CSVTMT_TK_TEXT,
	CSVTMT_TK_NOT,
	CSVTMT_TK_NULL,
} CsvTomatoTokenKind;

/********
* types *
********/

struct CsvTomatoError;
typedef struct CsvTomatoError CsvTomatoError;

struct CsvTomato;
typedef struct CsvTomato CsvTomato;

struct CsvTomatoStmt;
typedef struct CsvTomatoStmt CsvTomatoStmt;

struct CsvTomatoToken;
typedef struct CsvTomatoToken CsvTomatoToken;

struct CsvTomatoTokenizer;
typedef struct CsvTomatoTokenizer CsvTomatoTokenizer;

/**********
* structs *
**********/

struct CsvTomatoError {
	bool error;
	CsvTomatoErrorKind kind;
	char message[CSVTMT_ERR_MSG_SIZE];
};

struct CsvTomato {
	char db_dir[CSVTMT_STR_SIZE];
};

struct CsvTomatoStmt {
	int a;
};

struct CsvTomatoToken {
	CsvTomatoTokenKind kind;
	char text[CSVTMT_STR_SIZE];
	size_t len;
	struct CsvTomatoToken *next;
};

struct CsvTomatoTokenizer {
	CsvTomatoToken *root_token;
	size_t index;
	size_t len;
	const char *code;
};

/*************
* prototypes *
*************/


