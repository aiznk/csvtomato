#include <csvtomato.h>

CsvTomatoStmt *
csvtmt_stmt_new(CsvTomatoError *error) {
	CsvTomatoStmt *self = calloc(1, sizeof(*self));
	if (!self) {
		goto failed_to_calloc;
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
		goto fail;
	}

	return result;
fail:
	return CSVTMT_ERROR;
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
	if (error->error) {
		goto fail;		
	}

	self->node = csvtmt_parser_parse(
		self->parser,
		self->token,
		error
	);
	if (error->error) {
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

	CsvTomatoStmt *stmt = csvtmt_stmt_new(error);
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
	*stmt = csvtmt_stmt_new(error);
	if (error->error) {
		return CSVTMT_ERROR;
	}

	CsvTomatoStmt *s = *stmt;

	snprintf(s->model.db_dir, sizeof s->model.db_dir, "%s", self->db_dir);

	return csvtmt_stmt_prepare(s, query, error);
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
csvtmt_bind_float(
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
				elem->kind = CSVTMT_OP_FLOAT_VALUE;
				elem->obj.float_value.value = value;
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
