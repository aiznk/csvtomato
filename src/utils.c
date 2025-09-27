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
	CsvTomatoError error = {0};
	CsvTomato *db = NULL;
	CsvTomatoStmt *stmt = NULL;

	#define check_error() {\
		if (error.error) {\
			if (stmt) {\
				csvtmt_stmt_del(stmt);\
			}\
			if (db) {\
				csvtmt_close(db);\
			}\
			csvtmt_error_show(&error);\
			return;\
		}\
	}\

	db = csvtmt_open(db_dir, &error);
	check_error();

	csvtmt_prepare(db, query, &stmt, &error);
	check_error();

	while (csvtmt_step(stmt, &error) == CSVTMT_ROW) {
		if (error.error) {
			csvtmt_error_show(&error);
			memset(&error, 0, sizeof(error));
		}
		for (size_t i = 0; i < stmt->model.selected_columns_len; i++) {
			const char *col = stmt->model.selected_columns[i];
			printf("%s ", col);
		}
		printf("\n");
	}
	if (error.error) {
		csvtmt_error_show(&error);
		memset(&error, 0, sizeof(error));
	}

	csvtmt_finalize(stmt);
	csvtmt_close(db);
}
