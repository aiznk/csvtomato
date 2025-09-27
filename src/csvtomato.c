#include <csvtomato.h>

CsvTomatoStmt *
csvtmt_stmt_new(const char *db_dir, CsvTomatoError *error) {
	CsvTomatoStmt *self = calloc(1, sizeof(*self));
	if (!self) {
		goto failed_to_calloc;
	}

	csvtmt_model_init(&self->model, db_dir, error);
	if (error->error) {
		goto fail;
	}

	self->tokenizer = csvtmt_tokenizer_new(error);
	if (error->error) {
		goto fail;
	}

	self->parser = csvtmt_parser_new(error);
	if (error->error) {
		goto fail;
	}

	self->opcode = csvtmt_opcode_new(error);
	if (error->error) {
		goto fail;		
	}

	self->executor = csvtmt_executor_new(error);
	if (error->error) {
		goto fail;		
	}

	return self;
fail:
	csvtmt_stmt_del(self);
	return NULL;
failed_to_calloc:
	csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory");
	return NULL;
}

void
csvtmt_stmt_del(CsvTomatoStmt *self) {
	csvtmt_token_del_all(self->token);
	csvtmt_node_del_all(self->node);
	csvtmt_parser_del(self->parser);
	csvtmt_tokenizer_del(self->tokenizer);
	csvtmt_opcode_del(self->opcode);
	csvtmt_executor_del(self->executor);
	csvtmt_model_final(&self->model);
	free(self);
}

CsvTomatoResult
csvtmt_stmt_step(CsvTomatoStmt *self, CsvTomatoError *error) {
	CsvTomatoResult result = csvtmt_executor_exec(
		self->executor,
		&self->model,
		self->opcode->elems,
		self->opcode->len,
		error
	);
	if (error->error) {
		return CSVTMT_ERROR;
	}

	return result;
}


CsvTomatoResult
csvtmt_stmt_prepare(
	CsvTomatoStmt *self,
	const char *query,
	CsvTomatoError *error
) {
	self->token = csvtmt_tokenizer_tokenize(
		self->tokenizer,
		query,
		error
	);
	if (!self->token || error->error) {
		goto fail;		
	}

	self->node = csvtmt_parser_parse(
		self->parser,
		self->token,
		error
	);
	if (!self->node || error->error) {
		goto fail;		
	}

	csvtmt_opcode_parse(
		self->opcode,
		self->node,
		error
	);
	if (error->error) {
		goto fail;
	}

	return CSVTMT_OK;
fail:
	return CSVTMT_ERROR;
}

CsvTomato *
csvtmt_open(
	const char *db_dir,
	CsvTomatoError *error
) {
	errno = 0;
	CsvTomato *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	if (!csvtmt_file_exists(db_dir)) {
		free(self);
		csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "%s database does not exists", db_dir);
		return NULL;
	}

	snprintf(self->db_dir, sizeof self->db_dir, "%s", db_dir);

	return self;
}

void
csvtmt_close(CsvTomato *self) {
	if (!self) {
		return;
	}

	free(self);
}

CsvTomatoResult
csvtmt_exec(
	CsvTomato *self,
	const char *query,
	CsvTomatoError *error
) {
	CsvTomatoResult result;

	CsvTomatoStmt *stmt = csvtmt_stmt_new(self->db_dir, error);
	if (error->error) {
		goto fail;
	}

	csvtmt_stmt_prepare(stmt, query, error);
	if (error->error) {
		goto fail;
	}

	result = csvtmt_stmt_step(stmt, error);
	if (error->error) {
		goto fail;
	}

	csvtmt_stmt_del(stmt);

	return result;
fail:
	return CSVTMT_ERROR;
}

CsvTomatoResult
csvtmt_prepare(
	CsvTomato *self, 
	const char *query, 
	CsvTomatoStmt **stmt, 
	CsvTomatoError *error
) {
	*stmt = csvtmt_stmt_new(self->db_dir, error);
	if (error->error) {
		return CSVTMT_ERROR;
	}

	return csvtmt_stmt_prepare(*stmt, query, error);
}

void
csvtmt_free(void *ptr) {
	free(ptr);
}

void
csvtmt_static(void *ptr) {
	// pass
}

void
csvtmt_bind_text(
	CsvTomatoStmt *stmt,
	size_t index, 
	const char *text, 
	ssize_t size, 
	void (*destructor)(void*),
	CsvTomatoError *error
) {
	size_t count = 1;

	assert(stmt);
	assert(stmt->opcode);
	for (size_t i = 0; i < stmt->opcode->len; i++) {
		CsvTomatoOpcodeElem *elem = &stmt->opcode->elems[i];
		if (elem->kind == CSVTMT_OP_PLACE_HOLDER ||
			elem->old_kind == CSVTMT_OP_PLACE_HOLDER) {
			if (count == index) {
				elem->old_kind = elem->kind;
				elem->kind = CSVTMT_OP_STRING_VALUE;
				if (size == -1) {
					elem->obj.string_value.value = csvtmt_strdup(text, error);
				} else {
					char *s = calloc(size, sizeof(char));
					if (!s) {
						goto failed_to_calloc;
					}

					snprintf(s, size-1, "%s", text);

					elem->obj.string_value.value = csvtmt_move(s);
					elem->obj.string_value.destructor = destructor;
				}
				if (error->error) {
					return;
				}
				// puts("bind text");
				break;
			}
			count++;
		}
	}

	return;
failed_to_calloc:
	csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
	return;
}

void
csvtmt_bind_int(
	CsvTomatoStmt *stmt,
	size_t index, 
	int64_t value, 
	CsvTomatoError *error
) {
	size_t count = 1;

	for (size_t i = 0; i < stmt->opcode->len; i++) {
		CsvTomatoOpcodeElem *elem = &stmt->opcode->elems[i];
		if (elem->kind == CSVTMT_OP_PLACE_HOLDER ||
			elem->old_kind == CSVTMT_OP_PLACE_HOLDER) {
			if (count == index) {
				elem->old_kind = elem->kind;
				elem->kind = CSVTMT_OP_INT_VALUE;
				elem->obj.int_value.value = value;
				if (error->error) {
					return;
				}
				// puts("bind int");
				break;
			}
			count++;
		}
	}
}

void
csvtmt_bind_double(
	CsvTomatoStmt *stmt,
	size_t index, 
	double value, 
	CsvTomatoError *error
) {
	size_t count = 1;

	for (size_t i = 0; i < stmt->opcode->len; i++) {
		CsvTomatoOpcodeElem *elem = &stmt->opcode->elems[i];
		if (elem->kind == CSVTMT_OP_PLACE_HOLDER ||
			elem->old_kind == CSVTMT_OP_PLACE_HOLDER) {
			if (count == index) {
				elem->old_kind = elem->kind;
				elem->kind = CSVTMT_OP_DOUBLE_VALUE;
				elem->obj.double_value.value = value;
				if (error->error) {
					return;
				}
				break;
			}
			count++;
		}
	}
}

CsvTomatoResult
csvtmt_step(CsvTomatoStmt *stmt, CsvTomatoError *error) {
	CsvTomatoResult result;

	csvtmt_row_final(&stmt->model.row);
	memset(&stmt->model.row, 0, sizeof(stmt->model.row));

	result = csvtmt_executor_exec(
		stmt->executor,
		&stmt->model,
		stmt->opcode->elems,
		stmt->opcode->len,
		error
	);
	if (error->error) {
		return CSVTMT_ERROR;
	}
	return result;
}

void
csvtmt_finalize(CsvTomatoStmt *stmt) {
	csvtmt_stmt_del(stmt);
}

int
csvtmt_column_int(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error) {
	if (!stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "row columns length is 0");
		return -1;
	}

	if (index >= stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "index out of range");
		return -1;
	}

	return atoi(stmt->model.selected_columns[index]);
}

double
csvtmt_column_double(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error) {
	if (!stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "row columns length is 0");
		return -1.0;
	}

	if (index >= stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "index out of range");
		return -1.0;
	}

	return atof(stmt->model.selected_columns[index]);
}

const char *
csvtmt_column_text(CsvTomatoStmt *stmt, size_t index, CsvTomatoError *error) {
	if (!stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "row columns length is 0");
		return NULL;
	}

	if (index >= stmt->model.selected_columns_len) {
		csvtmt_error_format(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "index out of range");
		return NULL;
	}

	return stmt->model.selected_columns[index];
}
