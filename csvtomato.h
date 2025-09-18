#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdarg.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>

/************
* constants *
************/

enum {
	CSVTMT_ERR_MSG_SIZE = 256,
	CSVTMT_STR_SIZE = 256,
	CSVTMT_IDENT_SIZE = 256,
};

typedef enum {
	CSVTMT_ERR_NONE,
	CSVTMT_ERR_MEM,
	CSVTMT_ERR_BUF_OVERFLOW,
	CSVTMT_ERR_TOKENIZE,
	CSVTMT_ERR_SYNTAX,
} CsvTomatoErrorKind;

typedef enum {
	CSVTMT_TK_NONE,
	CSVTMT_TK_ROOT,

	CSVTMT_TK_IDENT, // indent
	CSVTMT_TK_SEMICOLON, // ;
	CSVTMT_TK_ASSIGN, // =
	CSVTMT_TK_BEG_PAREN, // (
	CSVTMT_TK_END_PAREN, // )
	CSVTMT_TK_PLACE_HOLDER, // ?
	CSVTMT_TK_COMMA, // ,
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
	CSVTMT_TK_NULL,
	CSVTMT_TK_AUTOINCREMENT,
} CsvTomatoTokenKind;

typedef enum {
	CSVTMT_ND_NONE,
	CSVTMT_ND_STMT_LIST,
	CSVTMT_ND_STMT,
	CSVTMT_ND_CREATE_TABLE_STMT,
	CSVTMT_ND_COLUMN_DEF,
	CSVTMT_ND_COLUMN_CONSTRAINT,
} CsvTomatoNodeKind;

/*********
* macros *
*********/

#define CSVTMT_TRANSTENT csvtmt_free
#define CSVTMT_STATIC csvtmt_static

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

struct CsvTomatoNode;
typedef struct CsvTomatoNode CsvTomatoNode;

struct CsvTomatoParser;
typedef struct CsvTomatoParser CsvTomatoParser;

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

struct CsvTomatoNode {
	CsvTomatoNodeKind kind;
	struct CsvTomatoNode *next;
	union {
		struct {
			struct CsvTomatoNode *sql_stmts;
		} sql_stmt_list;
		struct {
			struct CsvTomatoNode *create_table_stmt;
		} sql_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_defs;
		} create_table_stmt;
		struct {
			char *column_name;
			CsvTomatoTokenKind type_name;
			struct CsvTomatoNode *column_constraint;
		} column_def;
		struct {
			bool primary;
			bool key;
			bool autoincrement;
			bool not_;
			bool null;
		} column_constraint;
	} obj;
};

struct CsvTomatoParser {
	CsvTomatoNode *root_node;
};

/*************
* prototypes *
*************/

// error.c 

void
csvtmt_error_format(
	CsvTomatoError *self,
	CsvTomatoErrorKind kind,
	const char *fmt,
	...
);

// utils.c

char *
csvtmt_strdup(const char *s, CsvTomatoError *error);

// csvtomato.c

CsvTomato *
csvtmt_open(const char *db_dir, CsvTomatoError *error);

void
csvtmt_execute(
	CsvTomato *db,
	const char *query,
	CsvTomatoError *error
);

void
csvtmt_prepare(
	CsvTomato *db, 
	const char *query, 
	CsvTomatoStmt **stmt, 
	CsvTomatoError *error
);

void
csvtmt_free(void *ptr);

void
csvtmt_static(void *ptr);

void
csvtmt_bind_text(
	CsvTomatoStmt *stmt,
	size_t index, 
	const char *text, 
	ssize_t size, 
	void (*destructor)(void*),
	CsvTomatoError *error
);

void
csvtmt_bind_int(
	CsvTomatoStmt *stmt,
	size_t index, 
	ssize_t value, 
	CsvTomatoError *error
);

void
csvtmt_step(CsvTomatoStmt *stmt, CsvTomatoError *error);

void
csvtmt_finalize(CsvTomatoStmt *stmt);

void
csvtmt_close(CsvTomato *self);

// tokenizer.c

CsvTomatoToken *
csvtmt_token_new(
	CsvTomatoTokenKind kind,
	CsvTomatoError *error
);

void
csvtmt_token_del(CsvTomatoToken *self);

void
csvtmt_token_del_all(CsvTomatoToken *self);

CsvTomatoTokenizer *
csvtmt_tokenizer_new(CsvTomatoError *error);

void
csvtmt_tokenizer_del(CsvTomatoTokenizer *self);

CsvTomatoToken *
csvtmt_tokenizer_tokenize(
	CsvTomatoTokenizer *self, 
	const char *query,
	CsvTomatoError *error
);

// parser.c

CsvTomatoNode *
csvtmt_node_new(CsvTomatoNodeKind kind, CsvTomatoError *error);

void
csvtmt_node_del(CsvTomatoNode *self);

void
csvtmt_node_del_all(CsvTomatoNode *self);

CsvTomatoParser *
csvtmt_parser_new(CsvTomatoError *error);

void
csvtmt_parser_del(CsvTomatoParser *self);

CsvTomatoNode *
csvtmt_parser_parse(
	CsvTomatoParser *self,
	CsvTomatoToken *token,
	CsvTomatoError *error
);
