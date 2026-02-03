#pragma once

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
	#include <dirent.h>
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

typedef enum {
	CSVTMT_DNT_ERROR,
	CSVTMT_DNT_UNKNOWN,
	CSVTMT_DNT_DIR,
} CsvTomatoDirNodeType;
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
	CSVTMT_NUM_STR_SIZE = 1024,
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
	CSVTMT_ERR_PARSE,
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
	CSVTMT_TK_COUNT,
	CSVTMT_TK_SHOW,
	CSVTMT_TK_TABLES,
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
	CSVTMT_ND_SHOW_STMT,
	CSVTMT_ND_SHOW_TABLES_STMT,
	CSVTMT_ND_FUNCTION,
	CSVTMT_ND_COUNT_FUNC,
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
	CSVTMT_OP_SHOW_TABLES_BEG,
	CSVTMT_OP_SHOW_TABLES_END,
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

struct CsvTomatoDir;
typedef struct CsvTomatoDir CsvTomatoDir;

struct CsvTomatoDirNode;
typedef struct CsvTomatoDirNode CsvTomatoDirNode;

struct CsvTomatoErrorElem;
typedef struct CsvTomatoErrorElem CsvTomatoErrorElem;

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

struct CsvTomatoRow;
typedef struct CsvTomatoRow CsvTomatoRow;

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

struct CsvTomatoColumnInfo;
typedef struct CsvTomatoColumnInfo CsvTomatoColumnInfo;

struct CsvTomatoColumnInfoArray;
typedef struct CsvTomatoColumnInfoArray CsvTomatoColumnInfoArray;

struct CsvTomatoRowArray;
typedef struct CsvTomatoRowArray CsvTomatoRowArray;

/************
* templates *
************/

#include "src/stringtmpl.h"
DECL_STRING(CsvTomatoString, csvtmt_str, char)

#include "src/arraytmpl.h"
DECL_ARRAY(CsvTomatoRows, csvtmt_rows, CsvTomatoRow)

/**********
* structs *
**********/

struct CsvTomatoDirNode {
#if defined(_WIN32)
    WIN32_FIND_DATA finddata;
#else
    struct dirent* node;
#endif
};

struct CsvTomatoDir {
#if defined(_WIN32)
    HANDLE handle;
    char dirpath[1024];
#else
    DIR* directory;
#endif
};

struct CsvTomatoErrorElem {
	CsvTomatoErrorKind kind;
	char message[CSVTMT_ERR_MSG_SIZE];
};

struct CsvTomatoError {
	bool error;
	CsvTomatoErrorElem elems[100];
	size_t len;
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
			struct CsvTomatoNode *show_stmt;
		} sql_stmt;
		struct {
			struct CsvTomatoNode *show_tables_stmt;
		} show_stmt;
		struct {
			char *db_name;
		} show_tables_stmt;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_def_list;
			bool if_not_exists;
		} create_table_stmt;
		struct {
			struct CsvTomatoNode *count_func;
		} function;
		struct {
			struct CsvTomatoNode *column_name;
		} count_func;
		struct {
			char *table_name;
			struct CsvTomatoNode *column_name_list;
			struct CsvTomatoNode *function;
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
			char *db_name;
		} show_tables_stmt;
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
	CSVTMT_STACK_ELEM_BOOL_VALUE,
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

struct CsvTomatoColumnInfo {
	const char *key;
	size_t index;
	CsvTomatoValue value;
};

struct CsvTomatoColumnInfoArray {
	CsvTomatoColumnInfo array[100];
	size_t len;
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
			bool value;
		} bool_value;
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

struct CsvTomatoRow {
	char *columns[CSVTMT_CSV_COLS_SIZE];
	size_t len;
};

struct CsvTomatoRowArray {
	CsvTomatoRow array[100];
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

typedef enum {
	CSVTMT_MODE_FIRST,
	CSVTMT_MODE_WHERE,
	CSVTMT_MODE_UPDATE_SET,
} CsvTomatoMode;

struct CsvTomatoModel {
	char db_dir[CSVTMT_PATH_SIZE];
	bool skip;
	size_t opcodes_index;
	size_t save_opcodes_index;
	CsvTomatoStackElem stack[CSVTMT_EXEC_STACK_SIZE];
	size_t stack_len;
	const char *table_name;
	const char *db_name;
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
	CsvTomatoRow row;
	CsvTomatoRows *rows;
	const char *selected_columns[CSVTMT_COLUMN_NAMES_ARRAY_SIZE];
	size_t selected_columns_len;
	struct {
		char *ptr;
		char *cur;
		int fd;
		size_t size;
	} mmap;
	FILE *fp;
	char *row_head;
	CsvTomatoMode mode;
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
csvtmt_error_push(
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

void
csvtmt_dir_node_del(CsvTomatoDirNode *self);

CsvTomatoDirNode *
csvtmt_dir_node_new(void);

const char *
csvtmt_dir_node_name(const CsvTomatoDirNode *self);

int
csvtmt_dir_close(CsvTomatoDir *self);

CsvTomatoDir *
csvtmt_dir_open(const char *path);

CsvTomatoDirNode *
csvtmt_dir_read(CsvTomatoDir *self);

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
csvtmt_row_show(CsvTomatoRow *self);

void
csvtmt_row_set_clone(
	CsvTomatoRow *self,
	size_t index,
	const char *col,
	CsvTomatoError *error
);

int
csvtmt_row_parse_stream(
	CsvTomatoRow *self,
	FILE *fp,
	CsvTomatoError *error
);

const char *
csvtmt_row_parse_string(
	CsvTomatoRow *self,
	const char *str,
	CsvTomatoError *error
);

void
csvtmt_row_append_to_stream(
	CsvTomatoRow *self,
	FILE *fp,
	bool wrap,
	CsvTomatoError *error
);

void
csvtmt_row_final(CsvTomatoRow *self);

// models.c

const char *
csvtmt_header_has_column_types(
	CsvTomatoHeader *self, 
	const char *column_names[], 
	size_t column_names_len,
	CsvTomatoError *error
);

const char *
csvtmt_header_has_key_values_types(
	CsvTomatoHeader *self, 
	CsvTomatoKeyValue key_values[], 
	size_t key_values_len,
	CsvTomatoError *error
);

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

CsvTomatoResult
csvtmt_show_tables(CsvTomatoModel *model, CsvTomatoError *error);

void
csvtmt_header_read_from_table(CsvTomatoHeader *self, const char *table_path, CsvTomatoError *error);

void
csvtmt_header_read_from_stream(CsvTomatoHeader *self, FILE *fp, CsvTomatoError *error);

const char *
csvtmt_header_read_from_string(CsvTomatoHeader *self, const char *p, CsvTomatoError *error);

void
csvtmt_open_mmap_for_read(
	CsvTomatoModel *model,
	const char *table_path,
	CsvTomatoError *error
);

void
csvtmt_open_mmap(
	CsvTomatoModel *model,
	const char *table_path,
	int fd_mode,
	int mmap_mode,
	CsvTomatoError *error
);

void
csvtmt_open_mmap_for_read_write(
	CsvTomatoModel *model,
	const char *table_path,
	CsvTomatoError *error
);

void
csvtmt_close_mmap(CsvTomatoModel *model);

void
csvtmt_parse_row_from_mmap(CsvTomatoModel *model, CsvTomatoError *error);

int
csvtmt_find_type_index(CsvTomatoModel *model, const char *type_name);

void
csvtmt_delete_row_head(CsvTomatoModel *model);

void
csvtmt_append_rows_to_table(
	const char *table_path,
	CsvTomatoRows *rows,
	bool wrap,
	CsvTomatoError *error
);

void
csvtmt_clear_rows(CsvTomatoRows *rows);

void
csvtmt_store_column_infos(
	CsvTomatoModel *model,
	CsvTomatoColumnInfoArray *infos,
	CsvTomatoKeyValue *kvs,
	size_t kvs_len,
	CsvTomatoError *error
);

void
csvtmt_replace_row(
	CsvTomatoModel *model,
	CsvTomatoRow *row,
	CsvTomatoColumnInfoArray *infos, 
	CsvTomatoError *error
);

int
csvtmt_update_all(CsvTomatoModel *model, CsvTomatoError *error);

bool
csvtmt_is_deleted_row(const CsvTomatoRow *row);

void
csvtmt_store_selected_columns(CsvTomatoModel *model, CsvTomatoRow *row, CsvTomatoError *error);
