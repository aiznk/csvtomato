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
	#include <sys/mman.h>
    #include <unistd.h>
    #include <utime.h>
	#include <fcntl.h>
	#include <signal.h>
    #define CSVTMT_MKDIR(path) mkdir(path, 0755)
#endif

/************
* constants *
************/

typedef enum {
	CSVTMT_OK,
	CSVTMT_ERROR,
	CSVTMT_DONE,
	CSVTMT_ROW,
} CsvTomatoResult;

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
	CSVTMT_EXEC_STACK_SIZE = 256,
	CSVTMT_ASSIGNS_ARRAY_SIZE = 128,
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
	CSVTMT_TK_STAR, // *
	CSVTMT_TK_COMMA, // ,
	CSVTMT_TK_INT,  // 123
	CSVTMT_TK_DOUBLE,  // 3.14
	CSVTMT_TK_STRING,

	// reserved idents
	CSVTMT_TK_CREATE,
	CSVTMT_TK_SELECT,
	CSVTMT_TK_INSERT,
	CSVTMT_TK_UPDATE,
	CSVTMT_TK_DELETE,
	CSVTMT_TK_FROM,
	CSVTMT_TK_SET,
	CSVTMT_TK_WHERE,
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
	CSVTMT_ND_SELECT_STMT,
	CSVTMT_ND_INSERT_STMT,
	CSVTMT_ND_UPDATE_STMT,
	CSVTMT_ND_DELETE_STMT,
	CSVTMT_ND_VALUES,
	CSVTMT_ND_EXPR,
	CSVTMT_ND_ASSIGN_EXPR,
	CSVTMT_ND_NUMBER,
	CSVTMT_ND_STRING,
	CSVTMT_ND_COLUMN_NAME,
	CSVTMT_ND_COLUMN_DEF,
	CSVTMT_ND_COLUMN_CONSTRAINT,
} CsvTomatoNodeKind;

typedef enum {
	CSVTMT_OP_NONE,
	CSVTMT_OP_STAR,
	CSVTMT_OP_CREATE_TABLE_STMT_BEG,
	CSVTMT_OP_CREATE_TABLE_STMT_END,
	CSVTMT_OP_SELECT_STMT_BEG,
	CSVTMT_OP_SELECT_STMT_END,
	CSVTMT_OP_INSERT_STMT_BEG,
	CSVTMT_OP_INSERT_STMT_END,
	CSVTMT_OP_UPDATE_STMT_BEG,
	CSVTMT_OP_UPDATE_STMT_END,
	CSVTMT_OP_UPDATE_SET_BEG,
	CSVTMT_OP_UPDATE_SET_END,
	CSVTMT_OP_WHERE_BEG,
	CSVTMT_OP_WHERE_END,
	CSVTMT_OP_DELETE_STMT_BEG,
	CSVTMT_OP_DELETE_STMT_END,
	CSVTMT_OP_COLUMN_NAMES_BEG,
	CSVTMT_OP_COLUMN_NAMES_END,
	CSVTMT_OP_ASSIGN,
	CSVTMT_OP_IDENT,
	CSVTMT_OP_VALUES_BEG,
	CSVTMT_OP_VALUES_END,
	CSVTMT_OP_INT_VALUE,
	CSVTMT_OP_DOUBLE_VALUE,
	CSVTMT_OP_STRING_VALUE,
	CSVTMT_OP_COLUMN_DEF,
	CSVTMT_OP_PLACE_HOLDER,
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

struct CsvTomatoColumnType;
typedef struct CsvTomatoColumnType CsvTomatoColumnType;

struct CsvTomatoColumnTypeDef;
typedef struct CsvTomatoColumnTypeDef CsvTomatoColumnTypeDef;

struct CsvTomatoHeader;
typedef struct CsvTomatoHeader CsvTomatoHeader;

struct CsvTomatoValue;
typedef struct CsvTomatoValue CsvTomatoValue;

struct CsvTomatoValues;
typedef struct CsvTomatoValues CsvTomatoValues;

struct CsvTomatoModel;
typedef struct CsvTomatoModel CsvTomatoModel;

struct CsvTomatoStackElem;
typedef struct CsvTomatoStackElem CsvTomatoStackElem;

struct CsvTomatoKeyValue;
typedef struct CsvTomatoKeyValue CsvTomatoKeyValue;

/**********
* structs *
**********/

struct CsvTomatoError {
	bool error;
	CsvTomatoErrorKind kind;
	char message[CSVTMT_ERR_MSG_SIZE];
};

struct CsvTomatoToken {
	CsvTomatoTokenKind kind;
	char text[CSVTMT_STR_SIZE];
	size_t len;
	int64_t int_value;
	double double_value;
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
			struct CsvTomatoNode *select_stmt;
			struct CsvTomatoNode *insert_stmt;
			struct CsvTomatoNode *update_stmt;
			struct CsvTomatoNode *delete_stmt;
		} sql_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_def_list;
			bool if_not_exists;
		} create_table_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_name_list;
			struct CsvTomatoNode *where_expr;
		} select_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_name_list;
			struct CsvTomatoNode *values_list;
		} insert_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *assign_expr_list;
			struct CsvTomatoNode *where_expr;
		} update_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *where_expr;
		} delete_stmt;
		struct {
			char *column_name;
			bool star;
		} column_name;
		struct {
			struct CsvTomatoNode *expr_list;
		} values;
		struct {
			struct CsvTomatoNode *assign_expr;
			struct CsvTomatoNode *number;
			struct CsvTomatoNode *string;
			bool place_holder;
		} expr;
		struct {
			char *ident;
			struct CsvTomatoNode *expr;
		} assign_expr;
		struct {
			int64_t int_value;
			double double_value;
			bool is_int;
			bool is_double;
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
	CsvTomatoOpcodeKind old_kind;
	union {
		struct {
			char *table_name;
			bool if_not_exists;
		} create_table_stmt;
		struct {
			char *table_name;
		} select_stmt;
		struct {
			char *table_name;
		} insert_stmt;
		struct {
			char *table_name;
		} update_stmt;
		struct {
			char *table_name;
		} delete_stmt;
		struct {
			char *value;
		} ident;
		struct {
			char *value;
			void (*destructor)(void *);
		} string_value;
		struct {
			int64_t value;
		} int_value;
		struct {
			double value;
		} double_value;
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

typedef enum {
	CSVTMT_STACK_ELEM_STRING_VALUE,
	CSVTMT_STACK_ELEM_INT_VALUE,
	CSVTMT_STACK_ELEM_DOUBLE_VALUE,
	CSVTMT_STACK_ELEM_IDENT,
	CSVTMT_STACK_ELEM_STAR,
	CSVTMT_STACK_ELEM_KEY_VALUE,
	CSVTMT_STACK_ELEM_COLUMN_NAMES_BEG,
	CSVTMT_STACK_ELEM_VALUES_BEG,
	CSVTMT_STACK_ELEM_UPDATE_SET_BEG,
	CSVTMT_STACK_ELEM_WHERE_BEG,
} CsvTomatoStackElemKind;

typedef enum {
	CSVTMT_VAL_NONE,
	CSVTMT_VAL_INT,
	CSVTMT_VAL_DOUBLE,
	CSVTMT_VAL_STRING,
	CSVTMT_VAL_PLACE_HOLDER,
} CsvTomatoValueKind;

struct CsvTomatoValue {
	CsvTomatoValueKind kind;
	int64_t int_value;
	double double_value;
	const char *string_value;
};

struct CsvTomatoStackElem {
	CsvTomatoStackElemKind kind;
	union {
		struct {
			const char *value;
		} string_value;
		struct {
			int64_t value;
		} int_value;
		struct {
			double value;
		} double_value;
		struct {
			const char *value;
		} ident;
		struct {
			const char *key;
			CsvTomatoValue value;
		} key_value;
	} obj;
};

struct CsvTomatoKeyValue {
	const char *key;
	CsvTomatoValue value;
};

struct CsvTomatoExecutor {
	int a;
};

struct CsvTomatoCsvLine {
	char *columns[CSVTMT_CSV_COLS_SIZE];
	size_t len;
};

struct CsvTomatoValues {
	CsvTomatoValue values[CSVTMT_VALUES_ARRAY_SIZE];
	size_t len;
};

struct CsvTomatoColumnTypeDef {
	bool integer;
	bool text;
	bool not_null;
	bool null;
	bool primary_key;
	bool autoincrement;
};

struct CsvTomatoColumnType {
	char type_name[CSVTMT_TYPE_NAME_SIZE];
	char type_def[CSVTMT_TYPE_DEF_SIZE];
	CsvTomatoColumnTypeDef type_def_info;
	size_t index;
};

struct CsvTomatoHeader {
	CsvTomatoColumnType types[CSVTMT_TYPES_ARRAY_SIZE];
	size_t types_len;
};

struct CsvTomatoModel {
	char db_dir[CSVTMT_PATH_SIZE];
	size_t opcodes_index;
	CsvTomatoStackElem stack[CSVTMT_EXEC_STACK_SIZE];
	size_t stack_len;
	const char *table_name;
	char table_path[CSVTMT_PATH_SIZE];
	bool do_create_table;
	const char *column_names[CSVTMT_COLUMN_NAMES_ARRAY_SIZE];
	size_t column_names_len;
	bool column_names_is_star;
	CsvTomatoValues values[CSVTMT_VALUES_ARRAY_SIZE];
	size_t values_len;
	CsvTomatoHeader header;
	CsvTomatoKeyValue update_set_key_values[CSVTMT_ASSIGNS_ARRAY_SIZE];
	size_t update_set_key_values_len;
	CsvTomatoKeyValue where_key_values[CSVTMT_ASSIGNS_ARRAY_SIZE];
	size_t where_key_values_len;
	CsvTomatoCsvLine row;
	const char *selected_columns[CSVTMT_COLUMN_NAMES_ARRAY_SIZE];
	size_t selected_columns_len;
	struct {
		char *ptr;
		char *cur;
		int fd;
		size_t size;
	} mmap;
};

struct CsvTomato {
	char db_dir[CSVTMT_PATH_SIZE];
};

struct CsvTomatoStmt {
	CsvTomatoTokenizer *tokenizer;
	CsvTomatoParser *parser;
	CsvTomatoExecutor *executor;
	CsvTomatoOpcode *opcode;
	CsvTomatoToken *token;
	CsvTomatoNode *node;
	CsvTomatoModel model;
};

/*************
* prototypes *
*************/

// error.c 

void
csvtmt_error_clear(CsvTomatoError *self);

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
csvtmt_wrap_column(const char *col, CsvTomatoError *error);

char *
csvtmt_strdup(const char *s, CsvTomatoError *error);

void
csvtmt_quick_exec(const char *db_dir, const char *query);

void _Noreturn
csvtmt_die(const char *s);

// csvtomato.c

CsvTomatoStmt *
csvtmt_stmt_new(const char *db_dir, CsvTomatoError *error);

void
csvtmt_stmt_del(CsvTomatoStmt *self);

CsvTomatoResult
csvtmt_stmt_prepare(CsvTomatoStmt *self, const char *query, CsvTomatoError *error);

CsvTomatoResult
csvtmt_stmt_step(CsvTomatoStmt *self, CsvTomatoError *error);

CsvTomato *
csvtmt_open(const char *db_dir, CsvTomatoError *error);

CsvTomatoResult
csvtmt_exec(
	CsvTomato *db,
	const char *query,
	CsvTomatoError *error
);

CsvTomatoResult
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
	int64_t value, 
	CsvTomatoError *error
);

void
csvtmt_bind_double(
	CsvTomatoStmt *stmt,
	size_t index, 
	double value, 
	CsvTomatoError *error
);

CsvTomatoResult
csvtmt_step(CsvTomatoStmt *stmt, CsvTomatoError *error);

int
csvtmt_column_int(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error);

double
csvtmt_column_double(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error);

const char *
csvtmt_column_text(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error);

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

CsvTomatoResult
csvtmt_executor_exec(
	CsvTomatoExecutor *self,
	CsvTomatoModel *model,
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoError *error
);

// file.c

char *
csvtmt_file_read(const char *path);

int 
csvtmt_file_remove(const char *path);

int
csvtmt_file_exists(const char *path);

int
csvtmt_file_mkdir(const char *path);

int
csvtmt_file_touch(const char *path);

int
csvtmt_file_rename(const char *old, const char *new);

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
csvtmt_csvline_show(CsvTomatoCsvLine *self);

void
csvtmt_csvline_set_clone(
	CsvTomatoCsvLine *self,
	size_t index,
	const char *col,
	CsvTomatoError *error
);

int
csvtmt_csvline_parse_stream(
	CsvTomatoCsvLine *self,
	FILE *fp,
	CsvTomatoError *error
);

const char *
csvtmt_csvline_parse_string(
	CsvTomatoCsvLine *self,
	const char *str,
	CsvTomatoError *error
);

void
csvtmt_csvline_append_to_stream(
	CsvTomatoCsvLine *self,
	FILE *fp,
	bool wrap,
	CsvTomatoError *error
);

void
csvtmt_csvline_final(CsvTomatoCsvLine *self);

// models.c

void
csvtmt_model_init(CsvTomatoModel *self, const char *db_dir, CsvTomatoError *error);

void
csvtmt_model_final(CsvTomatoModel *self);

CsvTomatoResult
csvtmt_select(CsvTomatoModel *model, CsvTomatoError *error);

CsvTomatoResult
csvtmt_insert(CsvTomatoModel *model, CsvTomatoError *error);

CsvTomatoResult
csvtmt_update(CsvTomatoModel *model, CsvTomatoError *error);

CsvTomatoResult
csvtmt_delete(CsvTomatoModel *model, CsvTomatoError *error);

