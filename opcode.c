#include "csvtomato.h"

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
	case CSVTMT_OP_NONE:
		break;
	case CSVTMT_OP_CREATE_TABLE_BEG:
		free(elem->obj.create_table_stmt.table_name);
		break;
	case CSVTMT_OP_CREATE_TABLE_END:
		break;
	case CSVTMT_OP_COLUMN_DEF:
		free(elem->obj.column_def.column_name);
		break;
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
static void opcode_column_def(CsvTomatoOpcode *self, CsvTomatoNode *node, CsvTomatoError *error);

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
}

static void
opcode_create_table_stmt(
	CsvTomatoOpcode *self, 
	CsvTomatoNode *node, 
	CsvTomatoError *error
) {
	assert(node);

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_CREATE_TABLE_BEG;
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

		elem.kind = CSVTMT_OP_CREATE_TABLE_END;

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
	assert(node);

	{
		CsvTomatoOpcodeElem elem = {0};

		elem.kind = CSVTMT_OP_CREATE_TABLE_BEG;
		elem.obj.insert_stmt.table_name = csvtmt_move(node->obj.insert_stmt.table_name);
		node->obj.insert_stmt.table_name = NULL;

		push(self, elem, error);
		if (error->error) {
			return;
		}
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
