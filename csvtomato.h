#pragma once

#include "src/stringtmpl.h"
DECL_STRING(CsvTomatoString, csvtmt_str, char)

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>
#include <assert.h>

#ifdef _WIN32
    #include <windows.h>
    #include <direct.h>
    #define CSVTMT_MKDIR(path) _mkdir(path)
#else
    #include <sys/stat.h>
    #include <sys/types.h>
    #include <unistd.h>
    #include <utime.h>
    #define CSVTMT_MKDIR(path) mkdir(path, 0755)
#endif

/************
* constants *
************/

enum {
	CSVTMT_ERR_MSG_SIZE = 256,
	CSVTMT_STR_SIZE = 256,
	CSVTMT_IDENT_SIZE = 256,
	CSVTMT_PATH_SIZE = 256,
	CSVTMT_COLUMN_NAMES_ARRAY_SIZE = 32,
	CSVTMT_VALUES_ARRAY_SIZE = 32,
	CSVTMT_TYPES_ARRAY_SIZE = 64,
	CSVTMT_TYPE_NAME_SIZE = 256,
	CSVTMT_TYPE_DEF_SIZE = 256,
	CSVTMT_CSV_COLS_SIZE = 128,
};

typedef enum {
	CSVTMT_ERR_NONE,
	CSVTMT_ERR_MEM,
	CSVTMT_ERR_BUF_OVERFLOW,
	CSVTMT_ERR_TOKENIZE,
	CSVTMT_ERR_SYNTAX,
	CSVTMT_ERR_EXEC,
	CSVTMT_ERR_FILE_IO,
	CSVTMT_ERR_INDEX_OUT_OF_RANGE,
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
	CSVTMT_ND_INSERT_STMT,
	CSVTMT_ND_VALUES,
	CSVTMT_ND_EXPR,
	CSVTMT_ND_NUMBER,
	CSVTMT_ND_STRING,
	CSVTMT_ND_COLUMN_NAME,
	CSVTMT_ND_COLUMN_DEF,
	CSVTMT_ND_COLUMN_CONSTRAINT,
} CsvTomatoNodeKind;

typedef enum {
	CSVTMT_OP_NONE,
	CSVTMT_OP_CREATE_TABLE_STMT_BEG,
	CSVTMT_OP_CREATE_TABLE_STMT_END,
	CSVTMT_OP_INSERT_STMT_BEG,
	CSVTMT_OP_INSERT_STMT_END,
	CSVTMT_OP_COLUMN_NAMES_BEG,
	CSVTMT_OP_COLUMN_NAMES_END,
	CSVTMT_OP_VALUES_BEG,
	CSVTMT_OP_VALUES_END,
	CSVTMT_OP_INT_VALUE,
	CSVTMT_OP_FLOAT_VALUE,
	CSVTMT_OP_STRING_VALUE,
	CSVTMT_OP_COLUMN_DEF,
} CsvTomatoOpcodeKind;

/*********
* macros *
*********/

#define CSVTMT_COL_MODE "__MODE__"
#define CSVTMT_TRANSTENT csvtmt_free
#define CSVTMT_STATIC csvtmt_static
#define csvtmt_move(o) o
#define csvtmt_numof(ary) (sizeof ary / sizeof ary[0])

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

struct CsvTomatoOpcodeElem;
typedef struct CsvTomatoOpcodeElem CsvTomatoOpcodeElem;

struct CsvTomatoOpcode;
typedef struct CsvTomatoOpcode CsvTomatoOpcode;

struct CsvTomatoExecutor;
typedef struct CsvTomatoExecutor CsvTomatoExecutor;

struct CsvTomatoStringList;
typedef struct CsvTomatoStringList CsvTomatoStringList;

struct CsvTomatoCsvLine;
typedef struct CsvTomatoCsvLine CsvTomatoCsvLine;

struct CsvTomatoModel;
typedef struct CsvTomatoModel CsvTomatoModel;

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
	int64_t int_value;
	double float_value;
	struct CsvTomatoToken *next;
};

struct CsvTomatoTokenizer {
	CsvTomatoToken *root_token;
	size_t index;
	size_t len;
	const char *code;
};

struct CsvTomatoStringList {
	char *str;
	struct CsvTomatoStringList *next;
};

struct CsvTomatoNode {
	CsvTomatoNodeKind kind;
	struct CsvTomatoNode *next;
	union {
		struct {
			struct CsvTomatoNode *sql_stmt_list;
		} sql_stmt_list;
		struct {
			struct CsvTomatoNode *create_table_stmt;
			struct CsvTomatoNode *insert_stmt;
		} sql_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_def_list;
			bool if_not_exists;
		} create_table_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_name_list;
			struct CsvTomatoNode *values_list;
		} insert_stmt;
		struct {
			char *column_name;
		} column_name;
		struct {
			struct CsvTomatoNode *expr_list;
		} values;
		struct {
			struct CsvTomatoNode *number;
			struct CsvTomatoNode *string;
		} expr;
		struct {
			int64_t int_value;
			double float_value;
			bool is_int;
			bool is_float;
		} number;
		struct {
			char *string;
		} string;
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

struct CsvTomatoOpcode {
	CsvTomatoOpcodeElem *elems;
	size_t capa;
	size_t len;
};

struct CsvTomatoOpcodeElem {
	CsvTomatoOpcodeKind kind;
	union {
		struct {
			char *table_name;
			bool if_not_exists;
		} create_table_stmt;
		struct {
			char *table_name;
		} insert_stmt;
		struct {
			char *value;
		} string_value;
		struct {
			int64_t value;
		} int_value;
		struct {
			double value;
		} float_value;
		struct {
			char *column_name;
			CsvTomatoTokenKind type_name;
			bool primary;
			bool key;
			bool autoincrement;
			bool not_;
			bool null;		
		} column_def;
	} obj;
};

struct CsvTomatoExecutor {
	int a;
};

struct CsvTomatoCsvLine {
	char *columns[CSVTMT_CSV_COLS_SIZE];
	size_t len;
};

typedef enum {
	CSVTMT_VAL_NONE,
	CSVTMT_VAL_INT,
	CSVTMT_VAL_FLOAT,
	CSVTMT_VAL_STRING,
} CsvTomatoValueKind;

typedef struct {
	CsvTomatoValueKind kind;
	int64_t int_value;
	double float_value;
	const char *string_value;
} CsvTomatoValue;

typedef struct {
	char type_name[CSVTMT_TYPE_NAME_SIZE];
	char type_def[CSVTMT_TYPE_DEF_SIZE];
	size_t index;
} CsvTomatoColumnType;

typedef struct {
	CsvTomatoColumnType types[CSVTMT_TYPES_ARRAY_SIZE];
	size_t types_len;
} CsvTomatoHeader;

typedef enum {
	CSVTMT_MODE_NONE,
	CSVTMT_MODE_COLUMN_NAMES,
	CSVTMT_MODE_VALUES,
} CsvTomatoMode;

struct CsvTomatoModel {
	char db_dir[CSVTMT_PATH_SIZE];
	const char *table_name;
	char table_path[CSVTMT_PATH_SIZE];
	bool do_create_table;
	const char *column_names[CSVTMT_COLUMN_NAMES_ARRAY_SIZE];
	size_t column_names_len;
	CsvTomatoValue values[CSVTMT_VALUES_ARRAY_SIZE];
	size_t values_len;
	CsvTomatoHeader header;
	CsvTomatoString *buf;
	CsvTomatoMode mode;
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

void
csvtmt_error_show(const CsvTomatoError *self);

const char *
csvtmt_error_msg(const CsvTomatoError *self);

// utils.c

char *
csvtmt_strdup(const char *s, CsvTomatoError *error);

void
csvtmt_quick_exec(const char *db_dir, const char *query);

void _Noreturn
csvtmt_die(const char *s);

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

// opcode.c

CsvTomatoOpcode *
csvtmt_opcode_new(CsvTomatoError *error);

void
csvtmt_opcode_del(CsvTomatoOpcode *self);

void
csvtmt_opcode_parse(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
);

// executor.c

CsvTomatoExecutor *
csvtmt_executor_new(CsvTomatoError *error);

void
csvtmt_executor_del(CsvTomatoExecutor *self);

void
csvtmt_executor_exec(
	CsvTomatoExecutor *self,
	CsvTomatoModel *model,
	const char *db_dir,
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoError *error
);

// file.c

int
csvtmt_file_exists(const char *path);

int
csvtmt_file_mkdir(const char *path);

int
csvtmt_file_touch(const char *path);

// stringlist.c 

CsvTomatoStringList *
csvtmt_strlist_new(CsvTomatoError *error);

void
csvtmt_strlist_del(CsvTomatoStringList *self);

void
csvtmt_strlist_move_back_str(
	CsvTomatoStringList *self, 
	char *move_str,
	CsvTomatoError *error
);

// csv.c

void
csvtmt_csvline_parse_stream(
	CsvTomatoCsvLine *self,
	FILE *fp,
	CsvTomatoError *error
);

void
csvtmt_csvline_destroy(CsvTomatoCsvLine *self);

// models.c

void
csvtmt_insert(CsvTomatoModel *model, CsvTomatoError *error);
