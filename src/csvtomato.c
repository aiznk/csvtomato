#include <csvtomato.h>

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

	snprintf(self->model.db_dir, sizeof self->model.db_dir, "%s", db_dir);

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
	csvtmt_close(self);
	return NULL;
}

void
csvtmt_close(CsvTomato *self) {
	if (!self) {
		return;
	}

	csvtmt_token_del_all(self->token);
	csvtmt_node_del_all(self->node);
	csvtmt_parser_del(self->parser);
	csvtmt_tokenizer_del(self->tokenizer);
	csvtmt_opcode_del(self->opcode);
	csvtmt_executor_del(self->executor);
	free(self);
}

void
csvtmt_execute(
	CsvTomato *self,
	const char *query,
	CsvTomatoError *error
) {
	self->token = csvtmt_tokenizer_tokenize(
		self->tokenizer,
		query,
		error
	);
	self->node = csvtmt_parser_parse(
		self->parser,
		self->token,
		error
	);
	csvtmt_opcode_parse(
		self->opcode,
		self->node,
		error
	);
	csvtmt_executor_exec(
		self->executor,
		&self->model,
		self->opcode->elems,
		self->opcode->len,
		error
	);
}

void
csvtmt_prepare(
	CsvTomato *db, 
	const char *query, 
	CsvTomatoStmt **stmt, 
	CsvTomatoError *error
) {

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

}

void
csvtmt_bind_int(
	CsvTomatoStmt *stmt,
	size_t index, 
	ssize_t value, 
	CsvTomatoError *error
) {

}

void
csvtmt_step(CsvTomatoStmt *stmt, CsvTomatoError *error) {

}

void
csvtmt_finalize(CsvTomatoStmt *stmt) {

}
