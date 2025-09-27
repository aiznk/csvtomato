#include <csvtomato.h>

CsvTomatoToken *
csvtmt_token_new(
	CsvTomatoTokenKind kind,
	CsvTomatoError *error
) {
	errno = 0;
	CsvTomatoToken *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;		
	}

	self->kind = kind;

	return self;
}

void
csvtmt_token_del(CsvTomatoToken *self) {
	if (!self) {
		return;
	}

	free(self);
}

void
csvtmt_token_del_all(CsvTomatoToken *self) {
	if (!self) {
		return;
	}

	for (CsvTomatoToken *cur = self; cur; ) {
		CsvTomatoToken *rm = cur;
		cur = cur->next;
		csvtmt_token_del(rm);
	}
}

CsvTomatoTokenizer *
csvtmt_tokenizer_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoTokenizer *self = calloc(1, sizeof(CsvTomatoTokenizer));
	if (!self) {
		csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	return self;
}

void
csvtmt_tokenizer_del(CsvTomatoTokenizer *self) {
	if (!self) {
		return;
	}

	free(self);
}

static CsvTomatoToken *
tokenize_ident(CsvTomatoTokenizer *self, CsvTomatoError *error) {
	CsvTomatoToken *tok = csvtmt_token_new(CSVTMT_TK_IDENT, error);
	if (error->error) {
		return NULL;
	}

	#undef push
	#define push(c) {\
		if (tok->len >= CSVTMT_STR_SIZE) {\
			csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "token buffer overflow (1)");\
			return NULL;\
		}\
		tok->text[tok->len++] = c;\
		tok->text[tok->len] = 0;\
	}\

	for (; self->index < self->len; self->index++) {
		char c1 = self->code[self->index];

		if (isalpha(c1)) {
			push(c1);
		} else {
			self->index--;
			break;
		}
	}

	if (!strcasecmp(tok->text, "create")) tok->kind = CSVTMT_TK_CREATE;
	else if (!strcasecmp(tok->text, "select")) tok->kind = CSVTMT_TK_SELECT;
	else if (!strcasecmp(tok->text, "update")) tok->kind = CSVTMT_TK_UPDATE;
	else if (!strcasecmp(tok->text, "delete")) tok->kind = CSVTMT_TK_DELETE;
	else if (!strcasecmp(tok->text, "from")) tok->kind = CSVTMT_TK_FROM;
	else if (!strcasecmp(tok->text, "set")) tok->kind = CSVTMT_TK_SET;
	else if (!strcasecmp(tok->text, "insert")) tok->kind = CSVTMT_TK_INSERT;
	else if (!strcasecmp(tok->text, "into")) tok->kind = CSVTMT_TK_INTO;
	else if (!strcasecmp(tok->text, "where")) tok->kind = CSVTMT_TK_WHERE;
	else if (!strcasecmp(tok->text, "values")) tok->kind = CSVTMT_TK_VALUES;
	else if (!strcasecmp(tok->text, "table")) tok->kind = CSVTMT_TK_TABLE;
	else if (!strcasecmp(tok->text, "if")) tok->kind = CSVTMT_TK_IF;
	else if (!strcasecmp(tok->text, "not")) tok->kind = CSVTMT_TK_NOT;
	else if (!strcasecmp(tok->text, "exists")) tok->kind = CSVTMT_TK_EXISTS;
	else if (!strcasecmp(tok->text, "integer")) tok->kind = CSVTMT_TK_INTEGER;
	else if (!strcasecmp(tok->text, "primary")) tok->kind = CSVTMT_TK_PRIMARY;
	else if (!strcasecmp(tok->text, "autoincrement")) tok->kind = CSVTMT_TK_AUTOINCREMENT;
	else if (!strcasecmp(tok->text, "key")) tok->kind = CSVTMT_TK_KEY;
	else if (!strcasecmp(tok->text, "text")) tok->kind = CSVTMT_TK_TEXT;
	else if (!strcasecmp(tok->text, "not")) tok->kind = CSVTMT_TK_NOT;
	else if (!strcasecmp(tok->text, "null")) tok->kind = CSVTMT_TK_NULL;

	return tok;
}

static CsvTomatoToken *
tokenize_string(
	CsvTomatoTokenizer *self, 
	CsvTomatoError *error,
	char quote
) {
	CsvTomatoToken *tok = csvtmt_token_new(CSVTMT_TK_STRING, error);
	if (error->error) {
		return NULL;
	}

	#undef push
	#define push(c) {\
		if (tok->len >= CSVTMT_STR_SIZE) {\
			csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "token buffer overflow (2)");\
			return NULL;\
		}\
		tok->text[tok->len++] = c;\
		tok->text[tok->len] = 0;\
	}\

	self->index++; // quote

	for (; self->index < self->len; self->index++) {
		char c1 = self->code[self->index];

		if (c1 == quote) {
			break;
		} else if (c1 == '\\') {
			self->index++;
			if (self->index < self->len) {
				c1 = self->code[self->index];
				push(c1);
			}
		} else {
			push(c1);
		}
	}

	return tok;
}

static CsvTomatoToken *
tokenize_number(
	CsvTomatoTokenizer *self, 
	CsvTomatoError *error
) {
	CsvTomatoToken *tok = csvtmt_token_new(CSVTMT_TK_INT, error);
	if (error->error) {
		return NULL;
	}

	#undef push
	#define push(c) {\
		if (tok->len >= CSVTMT_STR_SIZE) {\
			csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "token buffer overflow (3)");\
			return NULL;\
		}\
		tok->text[tok->len++] = c;\
		tok->text[tok->len] = 0;\
	}\

	for (; self->index < self->len; self->index++) {
		char c1 = self->code[self->index];

		if (isdigit(c1)) {
			push(c1);
		} else if (c1 == '.') {
			push(c1);
			tok->kind = CSVTMT_TK_DOUBLE;
		} else {
			self->index--;
			break;
		}
	}

	if (tok->kind == CSVTMT_TK_INT) {
		tok->int_value = atoi(tok->text);
	} else {
		tok->double_value = atof(tok->text);
	}

	return tok;
}

CsvTomatoToken *
csvtmt_tokenizer_tokenize(
	CsvTomatoTokenizer *self,
	const char *code,
	CsvTomatoError *error
) {
	self->index = 0;
	self->len = strlen(code);
	self->code = code;

	CsvTomatoToken *tok = csvtmt_token_new(CSVTMT_TK_ROOT, error);
	if (error->error) {
		return NULL;
	}

	CsvTomatoToken *root = tok;

	#define store(kind) {\
		tok->next = csvtmt_token_new(kind, error);\
		if (error->error) {\
			goto fail;\
		}\
		tok = tok->next;\
	}\

	for (; self->index < self->len; self->index++) {
		char c1 = self->code[self->index];

		if (isspace(c1)) {
			// pass
		} else if (isalpha(c1)) {
			tok->next = tokenize_ident(self, error);
			if (error->error) {
				goto fail;
			}
			tok = tok->next;
		} else if (isdigit(c1)) {
			tok->next = tokenize_number(self, error);
			if (error->error) {
				goto fail;
			}
			tok = tok->next;
		} else if (c1 == '"' || c1 == '\'') {
			tok->next = tokenize_string(self, error, c1);
			if (error->error) {
				goto fail;
			}
			tok = tok->next;			
		} else if (c1 == ';') {
			store(CSVTMT_TK_SEMICOLON);
		} else if (c1 == '=') {
			store(CSVTMT_TK_ASSIGN);
		} else if (c1 == '(') {
			store(CSVTMT_TK_BEG_PAREN);
		} else if (c1 == ')') {
			store(CSVTMT_TK_END_PAREN);
		} else if (c1 == '?') {
			store(CSVTMT_TK_PLACE_HOLDER);
		} else if (c1 == '*') {
			store(CSVTMT_TK_STAR);
		} else if (c1 == ',') {
			store(CSVTMT_TK_COMMA);
		} else if (c1 == '?') {
			store(CSVTMT_TK_PLACE_HOLDER);
		} else {
			csvtmt_error_push(error, CSVTMT_ERR_TOKENIZE, "not supported character '%c' on tokenize", c1);
			goto fail;
		}
	}

	return root;
fail:
	csvtmt_token_del_all(root);
	return NULL;
}
