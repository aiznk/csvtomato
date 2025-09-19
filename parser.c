#include "csvtomato.h"

CsvTomatoNode *
csvtmt_node_new(CsvTomatoNodeKind kind, CsvTomatoError *error) {
	errno = 0;
	CsvTomatoNode *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	self->kind = kind;

	return self;
}

void
csvtmt_node_del(CsvTomatoNode *self) {
	if (!self) {
		return;
	}

	free(self);
}

void
csvtmt_node_del_all(CsvTomatoNode *self) {
	if (!self) {
		return;
	}

	switch (self->kind) {
	case CSVTMT_ND_NONE:
		break;
	case CSVTMT_ND_STMT_LIST:
		// puts("CSVTMT_ND_STMT_LIST");
		for (CsvTomatoNode *cur = self->obj.sql_stmt_list.sql_stmt_list; cur; ) {
			CsvTomatoNode *rm = cur;
			cur = cur->next;
			csvtmt_node_del_all(rm);
		}
		break;
	case CSVTMT_ND_STMT:
		// puts("CSVTMT_ND_STMT");
		csvtmt_node_del_all(self->obj.sql_stmt.create_table_stmt);
		csvtmt_node_del_all(self->obj.sql_stmt.insert_stmt);
		break;
	case CSVTMT_ND_CREATE_TABLE_STMT:
		// puts("CSVTMT_ND_CREATE_TABLE_STMT");
		free(self->obj.create_table_stmt.table_name);
		for (CsvTomatoNode *cur = self->obj.create_table_stmt.column_def_list; cur; ) {
			CsvTomatoNode *rm = cur;
			cur = cur->next;
			csvtmt_node_del_all(rm);
		}
		break;
	case CSVTMT_ND_INSERT_STMT:
		free(self->obj.insert_stmt.table_name);

		for (CsvTomatoNode *cur = self->obj.insert_stmt.column_name_list; cur; ) {
			csvtmt_node_del_all(cur);
		}
		for (CsvTomatoNode *cur = self->obj.insert_stmt.values_list; cur; ) {
			csvtmt_node_del_all(cur);
		}
		break;
	case CSVTMT_ND_VALUES:
		for (CsvTomatoNode *cur = self->obj.values.expr_list; cur; ) {
			csvtmt_node_del_all(cur);
		}		
		break;
	case CSVTMT_ND_EXPR:
		csvtmt_node_del_all(self->obj.expr.number);
		csvtmt_node_del_all(self->obj.expr.string);
		break;
	case CSVTMT_ND_STRING:
		free(self->obj.string.string);
		break;
	case CSVTMT_ND_NUMBER:
		break;
	case CSVTMT_ND_COLUMN_NAME:
		free(self->obj.column_name.column_name);
		break;
	case CSVTMT_ND_COLUMN_DEF:
		// puts("CSVTMT_ND_COLUMN_DEF");
		free(self->obj.column_def.column_name);
		csvtmt_node_del_all(self->obj.column_def.column_constraint);
		break;
	case CSVTMT_ND_COLUMN_CONSTRAINT:
		// puts("CSVTMT_ND_COLUMN_CONSTRAINT");
		break;
	}

	free(self);
}

CsvTomatoParser *
csvtmt_parser_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoParser *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	return self;
}

void
csvtmt_parser_del(CsvTomatoParser *self) {
	if (!self) {
		return;
	}

	free(self);
}

static CsvTomatoNode *parse_sql_stmt_list(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_sql_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_create_table_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_insert_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_column_name(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_values(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_expr(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_number(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_string(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_column_def(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);
static CsvTomatoNode *parse_column_constraint(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error);

static void
node_push(CsvTomatoNode *node, CsvTomatoNode *n);

static inline bool
is_end(CsvTomatoToken **token) {
	return *token == NULL;
}

static inline void
next(CsvTomatoToken **token) {
	if (*token) {
		*token = (*token)->next;
	}
}

static inline CsvTomatoTokenKind
kind(CsvTomatoToken **token) {
	if (is_end(token)) {
		return CSVTMT_TK_NONE;
	}
	return (*token)->kind;
}

static inline const char *
text(CsvTomatoToken **token) {
	return (*token)->text;
}

static void
node_push(CsvTomatoNode *node, CsvTomatoNode *n) {
	CsvTomatoNode *tail = NULL;

	for (CsvTomatoNode *cur = node; cur; cur = cur->next) {
		if (!cur->next) {
			tail = cur;
			break;
		}
	}

	if (tail) {
		tail->next = n;
	}
}

CsvTomatoNode *
csvtmt_parser_parse(
	CsvTomatoParser *self,
	CsvTomatoToken *token,
	CsvTomatoError *error
) {
	CsvTomatoToken **tok = &token;

	if (kind(tok) == CSVTMT_TK_ROOT) {
		next(tok);
	} else {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found root token");
		return NULL;
	}
	return parse_sql_stmt_list(self, tok, error);
}

static CsvTomatoNode * 
parse_sql_stmt_list(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_STMT_LIST, error);
	if (error->error) {
		return NULL;
	}

	CsvTomatoNode *sql_stmt_list;

	sql_stmt_list = parse_sql_stmt(self, token, error);
	if (error->error) {
		csvtmt_node_del_all(n1);
		return NULL;
	}

	for (; !is_end(token) ;) {
		if (kind(token) == CSVTMT_TK_SEMICOLON) {
			next(token);
		} else {
			break;
		}

		CsvTomatoNode *sql_stmt = parse_sql_stmt(self, token, error);
		if (error->error) {
			goto fail;
		}
		node_push(sql_stmt_list, sql_stmt);
	}

	n1->obj.sql_stmt_list.sql_stmt_list = sql_stmt_list;
	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode * 
parse_sql_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_STMT, error);
	if (error->error) {
		return NULL;
	}

	n1->obj.sql_stmt.create_table_stmt = parse_create_table_stmt(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (n1->obj.sql_stmt.create_table_stmt) {
		return n1;
	}

	n1->obj.sql_stmt.insert_stmt = parse_insert_stmt(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (n1->obj.sql_stmt.insert_stmt) {
		return n1;
	}

fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode * 
parse_create_table_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoToken *save = *token;
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_CREATE_TABLE_STMT, error);
	if (error->error) {
		return NULL;
	}

	// CREATE
	if (kind(token) == CSVTMT_TK_CREATE) {
		next(token);
	} else {
		goto fail;
	}

	// TABLE
	if (kind(token) == CSVTMT_TK_TABLE) {
		next(token);
	} else {
		*token = save;
		goto fail;
	}

	// [ IF NOT EXISTS ]
	if (kind(token) == CSVTMT_TK_IF) {
		next(token);
		if (kind(token) == CSVTMT_TK_NOT) {
			next(token);
			if (kind(token) == CSVTMT_TK_EXISTS) {
				next(token);
				n1->obj.create_table_stmt.if_not_exists = true;
			} else {
				csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found EXISTS after IF NOT on CREATE TABLE");
				goto fail;
			}
		} else {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found NOT after IF on CREATE TABLE");
			goto fail;
		}
	}

	// table_name
	if (kind(token) != CSVTMT_TK_IDENT) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found table_name on CREATE TABLE");
		goto fail;
	}

	n1->obj.create_table_stmt.table_name = csvtmt_strdup(text(token), error);
	if (error->error) {
		goto fail;
	}
	next(token);

	// '(' column_def ( ',' column_def )* ')'
	if (kind(token) == CSVTMT_TK_BEG_PAREN) {
		next(token);
	} else {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found ( on CREATE TABLE");
		goto fail;
	}

	CsvTomatoNode *column_def_list;

	column_def_list = parse_column_def(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (!column_def_list) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found first column def on CREATE TABLE");
		goto fail;
	}

	for (; !is_end(token); ) {
		if (kind(token) == CSVTMT_TK_COMMA) {
			next(token);
		} else {
			break;
		}

		CsvTomatoNode *clmn = parse_column_def(self, token, error);
		if (error->error) {
			goto fail;
		}
		if (!clmn) {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found column_def after comma on CREATE TABLE");
			goto fail;
		}

		node_push(column_def_list, clmn);
	}

	n1->obj.create_table_stmt.column_def_list = column_def_list;

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static bool
is_valid_type_name(CsvTomatoTokenKind kind) {
	switch (kind) {
	default:
		return false;
	case CSVTMT_TK_INTEGER:
	case CSVTMT_TK_TEXT:
		return true;
	}
}

static CsvTomatoNode * 
parse_column_def(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_COLUMN_DEF, error);
	if (error->error) {
		return NULL;
	}

	if (kind(token) != CSVTMT_TK_IDENT) {
		goto fail;
	}
	n1->obj.column_def.column_name = csvtmt_strdup(text(token), error);
	if (error->error) {
		goto fail;
	}
	next(token);

	if (!is_valid_type_name(kind(token))) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "invalid type name: %d: %s", kind(token), text(token));
		goto fail;
	}
	n1->obj.column_def.type_name = kind(token);
	next(token);

	CsvTomatoNode *constraint;
	constraint = parse_column_constraint(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (!constraint) {
		goto ok;
	}

	n1->obj.column_def.column_constraint = constraint;

ok:
	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode * 
parse_column_constraint(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_COLUMN_CONSTRAINT, error);
	if (error->error) {
		return NULL;
	}

	if (kind(token) == CSVTMT_TK_PRIMARY) {
		next(token);
		n1->obj.column_constraint.primary = true;

		if (kind(token) == CSVTMT_TK_KEY) {
			next(token);
			n1->obj.column_constraint.key = true;

			if (kind(token) == CSVTMT_TK_AUTOINCREMENT) {
				next(token);
				n1->obj.column_constraint.autoincrement = true;
				goto ok;
			} else {
				goto ok;
			}
		} else {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found KEY after PRIMARY on column_constraint");
			goto fail;
		}
	} else if (kind(token) == CSVTMT_TK_NOT) {
		next(token);
		n1->obj.column_constraint.not_ = true;
		if (kind(token) == CSVTMT_TK_NULL) {
			next(token);
			n1->obj.column_constraint.null = true;
			goto ok;
		} else {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found NULL after NOT on column_constraint");
			goto fail;
		}
	} else {
		csvtmt_node_del_all(n1);
		return NULL;  // not error
	}

ok:
	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_insert_stmt(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	if (kind(token) != CSVTMT_TK_INSERT) {
		return NULL;
	}
	next(token);

	if (kind(token) != CSVTMT_TK_INTO) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found INTO after INSERT on insert statement");
		return NULL;
	}
	next(token);

	if (kind(token) != CSVTMT_TK_IDENT) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found table name after INSERT INTO on insert statement");
		return NULL;
	}

	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_INSERT_STMT, error);
	if (error->error) {
		return NULL;
	}

	n1->obj.insert_stmt.table_name = csvtmt_strdup(text(token), error);
	if (error->error) {
		goto fail;
	}

	if (kind(token) == CSVTMT_TK_BEG_PAREN) {
		// [ '(' column_name ( ',' column_name ) * ')' ]
		next(token);

		CsvTomatoNode *column_name_list = parse_column_name(self, token, error);
		if (error->error) {
			goto fail;
		}
		if (!column_name_list) {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found column name after table name on insert statement");
			goto fail;
		}

		for (; !is_end(token); ) {
			if (kind(token) != CSVTMT_TK_COMMA) {
				break;
			}
			next(token);

			CsvTomatoNode *column_name = parse_column_name(self, token, error);
			if (error->error) {
				goto fail;
			}
			if (!column_name) {
				csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found column name after comma on insert statement");
				goto fail;
			}

			node_push(column_name_list, column_name);
		}

		if (kind(token) != CSVTMT_TK_END_PAREN) {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found ) after column name on insert statement");
			goto fail;
		}
		next(token);

		n1->obj.insert_stmt.column_name_list = column_name_list;
	}

	if (kind(token) == CSVTMT_TK_VALUES) {
		next(token);

		CsvTomatoNode *values_list = parse_values(self, token, error);
		if (error->error) {
			goto fail;
		}
		if (!values_list) {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found values on insert statement");
			goto fail;
		}

		for (; !is_end(token); ) {
			if (kind(token) == CSVTMT_TK_COMMA) {
				next(token);
			} else {
				break;
			}

			CsvTomatoNode *values = parse_values(self, token, error);
			if (error->error) {
				goto fail;
			}
			if (!values) {
				csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found values after comma on insert statement");
				goto fail;
			}
		}

		n1->obj.insert_stmt.values_list = values_list;
	}

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_column_name(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	if (kind(token) != CSVTMT_TK_IDENT) {
		return NULL;
	}

	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_COLUMN_NAME, error);
	if (error->error) {
		return NULL;
	}

	n1->obj.column_name.column_name = csvtmt_strdup(text(token), error);
	if (error->error) {
		goto fail;
	}

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_values(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	if (kind(token) != CSVTMT_TK_BEG_PAREN) {
		return NULL;
	}
	next(token);

	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_VALUES, error);
	if (error->error) {
		return NULL;
	}

	CsvTomatoNode *expr_list = parse_expr(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (!expr_list) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found expression on VALUES");
		goto fail;
	}

	for (; !is_end(token); ) {
		if (kind(token) == CSVTMT_TK_COMMA) {
			next(token);
		} else {
			break;
		}

		CsvTomatoNode *expr = parse_expr(self, token, error);
		if (error->error) {
			goto fail;
		}
		if (!expr) {
			csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found expression after comma on VALUES");
			goto fail;
		}

		node_push(expr_list, expr);
	}

	if (kind(token) != CSVTMT_TK_END_PAREN) {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "not found ) on VALUES");
		return NULL;
	}
	next(token);

	n1->obj.values.expr_list = expr_list;

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_expr(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_EXPR, error);
	if (error->error) {
		return NULL;
	}

	CsvTomatoNode *n2;

	n2 = parse_number(self, token, error);
	if (error->error) {
		goto fail;
	}
	if (n2) {
		n1->obj.expr.number = n2;
	} else {
		n2 = parse_string(self, token, error);
		if (error->error) {
			goto fail;
		}
		if (n2) {
			n1->obj.expr.string = n2;
		} else {
			goto fail;
		}
	}

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_number(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_NUMBER, error);
	if (error->error) {
		return NULL;
	}	

	if (kind(token) == CSVTMT_TK_INT) {
		n1->obj.number.int_value = (*token)->int_value;
		n1->obj.number.is_int = true;
	} else if (kind(token) == CSVTMT_TK_FLOAT) {
		n1->obj.number.float_value = (*token)->float_value;
		n1->obj.number.is_float = true;
	} else {
		goto fail;
	}
	
	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}

static CsvTomatoNode *
parse_string(CsvTomatoParser *self, CsvTomatoToken **token, CsvTomatoError *error) {
	CsvTomatoNode *n1 = csvtmt_node_new(CSVTMT_ND_STRING, error);
	if (error->error) {
		return NULL;
	}

	if (kind(token) == CSVTMT_TK_STRING) {
		n1->obj.string.string = csvtmt_strdup(text(token), error);
		if (error->error) {
			goto fail;
		}
	} else {
		goto fail;
	}

	return n1;
fail:
	csvtmt_node_del_all(n1);
	return NULL;
}
