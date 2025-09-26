#include <csvtomato.h>

char *
csvtmt_wrap_column(const char *col, CsvTomatoError *error) {
	CsvTomatoString *s = csvtmt_str_new();
	if (!s) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate string");
		return NULL;
	}

	csvtmt_str_push_back(s, '"');

	for (const char *p = col; *p; p++) {
		if (*p == '"') {
			csvtmt_str_push_back(s, *p);
			csvtmt_str_push_back(s, *p);
		} else if (*p == '\\') {
			p++;
			if (*p) {
				csvtmt_str_push_back(s, *p);
			} else {
				break;
			}
		} else {
			csvtmt_str_push_back(s, *p);
		}
	}

	csvtmt_str_push_back(s, '"');

	return csvtmt_str_esc_del(s);
}

char *
csvtmt_strdup(const char *s, CsvTomatoError *error) {
	size_t len = strlen(s);

	errno = 0;
	char *p = malloc(len + 1);
	if (!p) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	strcpy(p, s);

	return p;
}

void
csvtmt_quick_exec(const char *db_dir, const char *query) {
	#define check_error() {\
		if (error.error) {\
			csvtmt_error_show(&error);\
			return;\
		}\
	}\

	CsvTomatoError error = {0};
	CsvTomatoTokenizer *t;
	CsvTomatoParser *p;
	CsvTomatoExecutor *e;
	CsvTomatoOpcode *o;
	CsvTomatoToken *token;
	CsvTomatoNode *node;
	CsvTomatoModel model = {0};

	t = csvtmt_tokenizer_new(&error);
	p = csvtmt_parser_new(&error);
	o = csvtmt_opcode_new(&error);
	e = csvtmt_executor_new(&error);

	token = csvtmt_tokenizer_tokenize(t, query, &error);
	check_error();

	node = csvtmt_parser_parse(p, token, &error);
	check_error();

	csvtmt_opcode_parse(o, node, &error);
	check_error();

	snprintf(model.db_dir, sizeof model.db_dir, "%s", db_dir);
	csvtmt_executor_exec(e, &model, o->elems, o->len, &error);
	check_error();

	csvtmt_token_del_all(token);
	csvtmt_node_del_all(node);
	csvtmt_parser_del(p);
	csvtmt_tokenizer_del(t);
	csvtmt_opcode_del(o);
	csvtmt_executor_del(e);
}
