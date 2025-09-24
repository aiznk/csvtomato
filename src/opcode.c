#include <csvtomato.h>

CsvTomatoOpcode *
csvtmt_opcode_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoOpcode *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocat memory: %s", strerror(errno));
		return NULL;
	}

	self->capa = 100;
	size_t byte = sizeof(CsvTomatoOpcodeElem);
	self->elems = calloc(self->capa + 1, byte);
	if (!self->elems) {
		free(self);
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocat memory (2): %s", strerror(errno));
		return NULL;
	}

	return self;
}

static void
destroy_elem(CsvTomatoOpcodeElem *elem) {
	switch (elem->kind) {
	case CSVTMT_OP_NONE: break;
	case CSVTMT_OP_PLACE_HOLDER: break;
	case CSVTMT_OP_ASSIGN: break;
	case CSVTMT_OP_CREATE_TABLE_STMT_BEG:
		free(elem->obj.create_table_stmt.table_name);
		break;
	case CSVTMT_OP_CREATE_TABLE_STMT_END: break;
	case CSVTMT_OP_INSERT_STMT_BEG:
		free(elem->obj.insert_stmt.table_name);
		break;
	case CSVTMT_OP_INSERT_STMT_END: break;
	case CSVTMT_OP_UPDATE_STMT_BEG:
		free(elem->obj.update_stmt.table_name);
		break;
	case CSVTMT_OP_UPDATE_STMT_END: break;
	case CSVTMT_OP_UPDATE_SET_BEG: break;
	case CSVTMT_OP_UPDATE_SET_END: break;
	case CSVTMT_OP_DELETE_STMT_BEG:
		free(elem->obj.update_stmt.table_name);
		break;
	case CSVTMT_OP_DELETE_STMT_END: break;
	case CSVTMT_OP_WHERE_BEG: break;
	case CSVTMT_OP_WHERE_END: break;
	case CSVTMT_OP_IDENT:
		free(elem->obj.ident.value);
		break;
	case CSVTMT_OP_STRING_VALUE:
		if (elem->obj.string_value.destructor) {
			elem->obj.string_value.destructor(elem->obj.string_value.value);
		} else {
			free(elem->obj.string_value.value);
		}
		break;
	case CSVTMT_OP_INT_VALUE: break;
	case CSVTMT_OP_FLOAT_VALUE: break;
	case CSVTMT_OP_COLUMN_DEF:
		free(elem->obj.column_def.column_name);
		break;
	case CSVTMT_OP_COLUMN_NAMES_BEG: break;
	case CSVTMT_OP_COLUMN_NAMES_END: break;
	case CSVTMT_OP_VALUES_BEG: break;
	case CSVTMT_OP_VALUES_END: break;
	}
}

void
csvtmt_opcode_del(CsvTomatoOpcode *self) {
	for (size_t i = 0; i < self->len; i++) {
		CsvTomatoOpcodeElem *elem = &self->elems[i];
		destroy_elem(elem);
	}
	free(self->elems);
	free(self);
}

static void
resize(CsvTomatoOpcode *self, size_t new_capa, CsvTomatoError *error) {
	size_t byte = sizeof(CsvTomatoOpcodeElem);
	size_t size = byte * new_capa + byte;
	errno = 0;
	void *elems = realloc(self->elems, size);
	if (!elems) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to re-allocate memory: %s", strerror(errno));
		return;
	}

	self->elems = elems;
	self->capa = new_capa;
}

static void
push(
	CsvTomatoOpcode *self,
	CsvTomatoOpcodeElem elem,
	CsvTomatoError *error
) {
	if (self->len >= self->capa) {
		resize(self, self->capa*2, error);
		if (error->error) {
			return;
		}
	}
	self->elems[self->len++] = elem;
}

static void opcode_sql_stmt_list(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_sql_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_create_table_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_insert_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_update_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_delete_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_column_def(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_column_name(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_values(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_expr(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_assign_expr(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_number(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);
static void opcode_string(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);

void
csvtmt_opcode_parse(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	opcode_sql_stmt_list(self, node, error);
}

static void
opcode_sql_stmt_list(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error) {
	assert(node);
	CsvTomatoNode *cur = node->obj.sql_stmt_list.sql_stmt_list;
	for (; cur; cur = cur->next) {
		opcode_sql_stmt(self, cur, error);
		if (error->error) {
			return;
		}
	}		
}

static void
opcode_sql_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error) {
	assert(node);
	opcode_create_table_stmt(self, node->obj.sql_stmt.create_table_stmt, error);
	opcode_insert_stmt(self, node->obj.sql_stmt.insert_stmt, error);
	opcode_update_stmt(self, node->obj.sql_stmt.update_stmt, error);
	opcode_delete_stmt(self, node->obj.sql_stmt.delete_stmt, error);
}

static void
opcode_create_table_stmt(
	CsvTomatoOpcode *self, 
	CsvTomatoNode *node, 
	CsvTomatoError *error
) {
	if (!node) {
		return;
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_CREATE_TABLE_STMT_BEG;
		elem.obj.create_table_stmt.if_not_exists = node->obj.create_table_stmt.if_not_exists;
		elem.obj.create_table_stmt.table_name = csvtmt_move(node->obj.create_table_stmt.table_name);
		node->obj.create_table_stmt.table_name = NULL;

		push(self, elem, error);
		if (error->error) {
			return;
		}
	}

	CsvTomatoNode *cur = node->obj.create_table_stmt.column_def_list;
	for (; cur; cur = cur->next) {
		opcode_column_def(self, cur, error);
		if (error->error) {
			return;
		}
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_CREATE_TABLE_STMT_END;

		push(self, elem, error);
		if (error->error) {
			return;
		}
	}
}

static void
opcode_insert_stmt(
	CsvTomatoOpcode *self, 
	CsvTomatoNode *node, 
	CsvTomatoError *error
) {
	if (!node) {
		return;
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_INSERT_STMT_BEG;
		elem.obj.insert_stmt.table_name = csvtmt_move(node->obj.insert_stmt.table_name);
		node->obj.insert_stmt.table_name = NULL;

		push(self, elem, error);
		if (error->error) {
			return;
		}
	}

	{
		CsvTomatoOpcodeElem elem = {
			.kind = CSVTMT_OP_COLUMN_NAMES_BEG,
		};
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}

	opcode_column_name(self, node->obj.insert_stmt.column_name_list, error);
	if (error->error) {
		return;
	}

	{
		CsvTomatoOpcodeElem elem = {
			.kind = CSVTMT_OP_COLUMN_NAMES_END,
		};
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}

	for (CsvTomatoNode *values = node->obj.insert_stmt.values_list; values; values = values->next) {
		{
			CsvTomatoOpcodeElem elem = {
				.kind = CSVTMT_OP_VALUES_BEG,
			};
			push(self, elem, error);
			if (error->error) {
				return;
			}
		}

		opcode_values(self, values, error);
		if (error->error) {
			return;
		}		

		{
			CsvTomatoOpcodeElem elem = {
				.kind = CSVTMT_OP_VALUES_END,
			};
			push(self, elem, error);
			if (error->error) {
				return;
			}
		}
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_INSERT_STMT_END;
		elem.obj.insert_stmt.table_name = csvtmt_move(node->obj.insert_stmt.table_name);
		node->obj.insert_stmt.table_name = NULL;

		push(self, elem, error);
		if (error->error) {
			return;
		}
	}
}

static void
opcode_delete_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error) {
	if (!node) {
		return;
	}
	assert(node->kind == CSVTMT_ND_DELETE_STMT);

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_DELETE_STMT_BEG;
		elem.obj.delete_stmt.table_name = csvtmt_move(node->obj.delete_stmt.table_name);
		node->obj.delete_stmt.table_name = NULL;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	if (node->obj.delete_stmt.where_expr) {
		{
			CsvTomatoOpcodeElem elem = {0};

			elem.kind = CSVTMT_OP_WHERE_BEG;
			push(self, elem, error);
			if (error->error) {
				return;
			}
		}	

		opcode_expr(self, node->obj.delete_stmt.where_expr, error);
		if (error->error) {
			return;
		}

		{
			CsvTomatoOpcodeElem elem = {0};

			elem.kind = CSVTMT_OP_WHERE_END;
			push(self, elem, error);
			if (error->error) {
				return;
			}
		}	
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_DELETE_STMT_END;
		elem.obj.delete_stmt.table_name = csvtmt_move(node->obj.delete_stmt.table_name);
		node->obj.delete_stmt.table_name = NULL;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	
}

static void
opcode_update_stmt(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error) {
	if (!node) {
		return;
	}
	assert(node->kind == CSVTMT_ND_UPDATE_STMT);

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_UPDATE_STMT_BEG;
		elem.obj.update_stmt.table_name = csvtmt_move(node->obj.update_stmt.table_name);
		node->obj.update_stmt.table_name = NULL;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_UPDATE_SET_BEG;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	for (CsvTomatoNode *assign_expr = node->obj.update_stmt.assign_expr_list; assign_expr; assign_expr = assign_expr->next) {
		opcode_assign_expr(self, assign_expr, error);
		if (error->error) {
			return;
		}
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_UPDATE_SET_END;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_WHERE_BEG;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	if (node->obj.update_stmt.where_expr) {
		opcode_expr(self, node->obj.update_stmt.where_expr, error);
		if (error->error) {
			return;
		}
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_WHERE_END;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_UPDATE_STMT_END;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	
}

static void
opcode_assign_expr(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	if (!node) {
		return;
	}

	// IDENT, INT, ASSIGN
	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_IDENT;
		elem.obj.ident.value = csvtmt_move(node->obj.assign_expr.ident);
		node->obj.assign_expr.ident = NULL;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	

	// stack [IDENT, INT(1), INT(2)]
	// array: IDENT, INT, INT, ADD, ASSIGN
	// stack [IDENT] INT(1) + INT(2)
	// stack [IDENT, INT(3)]
	// array: ASSIGN
	// [] IDENT = INT(3)
	opcode_expr(self, node->obj.assign_expr.expr, error);
	if (error->error) {
		return;
	}

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_ASSIGN;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}	
}

static void
opcode_column_name(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	assert(node);
	assert(node->kind == CSVTMT_ND_COLUMN_NAME);

	CsvTomatoOpcodeElem elem = {0};

	elem.kind = CSVTMT_OP_STRING_VALUE;
	elem.obj.string_value.value = csvtmt_move(node->obj.column_name.column_name);
	node->obj.column_name.column_name = NULL;

	push(self, elem, error);
	if (error->error) {
		return;
	}

	if (node->next) {
		opcode_column_name(self, node->next, error);
		if (error->error) {
			return;
		}
	}
}

static void
opcode_values(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	assert(node);
	assert(node->kind == CSVTMT_ND_VALUES);

	for (CsvTomatoNode *expr = node->obj.values.expr_list; expr; expr = expr->next) {
		opcode_expr(self, expr, error);
		if (error->error) {
			return;
		}
	}
}

static void
opcode_expr(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	assert(node);
	assert(node->kind == CSVTMT_ND_EXPR);

	opcode_assign_expr(self, node->obj.expr.assign_expr, error);
	if (error->error) {
		return;
	}

	opcode_number(self, node->obj.expr.number, error);
	if (error->error) {
		return;
	}

	opcode_string(self, node->obj.expr.string, error);
	if (error->error) {
		return;
	}

	if (node->obj.expr.place_holder) {
		CsvTomatoOpcodeElem elem = {0};
		elem.kind = CSVTMT_OP_PLACE_HOLDER;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	}
}

static void
opcode_number(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	if (!node) {
		return;
	}
	assert(node->kind == CSVTMT_ND_NUMBER);

	CsvTomatoOpcodeElem elem = {0};

	if (node->obj.number.is_int) {
		elem.kind = CSVTMT_OP_INT_VALUE;
		elem.obj.int_value.value = node->obj.number.int_value;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	} else if (node->obj.number.is_float) {
		elem.kind = CSVTMT_OP_FLOAT_VALUE;
		elem.obj.float_value.value = node->obj.number.float_value;
		push(self, elem, error);
		if (error->error) {
			return;
		}
	} else {
		csvtmt_error_format(error, CSVTMT_ERR_SYNTAX, "invalid state in number");
		return;
	}
}

static void
opcode_string(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	if (!node) {
		return;
	}
	assert(node->kind == CSVTMT_ND_STRING);

	CsvTomatoOpcodeElem elem = {0};

	elem.kind = CSVTMT_OP_STRING_VALUE;
	elem.obj.string_value.value = csvtmt_move(node->obj.string.string);
	node->obj.string.string = NULL;
	push(self, elem, error);
	if (error->error) {
		return;
	}
}

static void
opcode_column_def(
	CsvTomatoOpcode *self,
	CsvTomatoNode *node,
	CsvTomatoError *error
) {
	assert(node);
	assert(node->kind == CSVTMT_ND_COLUMN_DEF);
	char *column_name = csvtmt_move(node->obj.column_def.column_name);
	node->obj.column_def.column_name = NULL;

	CsvTomatoTokenKind type_name = node->obj.column_def.type_name;
	CsvTomatoNode *cons = node->obj.column_def.column_constraint;
	CsvTomatoOpcodeElem elem = {0};
	
	elem.kind = CSVTMT_OP_COLUMN_DEF;
	elem.obj.column_def.column_name = csvtmt_move(column_name);
	elem.obj.column_def.type_name = type_name;

	if (cons) {
		elem.obj.column_def.primary = cons->obj.column_constraint.primary;
		elem.obj.column_def.key = cons->obj.column_constraint.key;
		elem.obj.column_def.autoincrement = cons->obj.column_constraint.autoincrement;
		elem.obj.column_def.not_ = cons->obj.column_constraint.not_;
		elem.obj.column_def.null = cons->obj.column_constraint.null;
	}

	push(self, elem, error);
	if (error->error) {
		return;
	}
}
