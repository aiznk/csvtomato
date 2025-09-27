#include <csvtomato.h>

void
csvtmt_row_show(CsvTomatoRow *self) {
	for (size_t i = 0; i < self->len; i++) {
		printf("[%s]", self->columns[i]);
	}
	printf("\n");
}

const char *
csvtmt_row_parse_string(
	CsvTomatoRow *self,
	const char *str,
	CsvTomatoError *error
) {
	#undef store
	#define store() {\
		if (any_read) {\
			if (self->len >= csvtmt_numof(self->columns)) {\
				csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "csv line columns overflow");\
				goto fail;\
			}\
			char *s = csvtmt_str_esc_del(buf);\
			self->columns[self->len++] = csvtmt_move(s);\
			buf = csvtmt_str_new();\
			any_read = false;\
		}\
	}\

	#undef push
	#define push(c) {\
		if (!csvtmt_str_push_back(buf, c)) {\
			csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to push back to string buffer");\
			goto fail;\
		}\
	}\

	#undef check_crlf
	#define check_crlf() {\
		c = *p++;\
		if (c == EOF) {\
			goto done;\
		} else if (c == '\n') {\
			goto done;\
		} else {\
			p--;\
			goto done;\
		}	\
	}\

	errno = 0;
	CsvTomatoString *buf = csvtmt_str_new();
	if (!buf) {
		csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate string: %s", strerror(errno));
		return NULL;
	}

	bool any_read = false;
	int sep = ',';
	int m = 0;
	const char *p = str;

	for (; *p; ) {
		int c = *p++;

		switch (m) {
		case 0:
			if (c == '"') {
				m = 10;
				any_read = true;
			} else if (c == sep) {
				any_read = true;
				store();
			} else if (c == '\n') {
				any_read = true;
				goto done;
			} else if (c == '\r') {
				any_read = true;
				check_crlf();
			} else {
				any_read = true;
				push(c);
				m = 30;
			}
			break;
		case 10: // found begin "
			if (c == '"') { 
				c = *p++;
				if (c == '"') {
					push(c);
				} else {
					p--;
					m = 20;
				}
			} else {
				push(c);
			}
			break;
		case 20: // found end "
			if (c == sep) {
				store();
				m = 0;
			} else if (c == '\n') {
				goto done;
			} else if (c == '\r') {
				check_crlf();
			} else {
				// ignore
			}
			break;
		case 30: // found normal character
			if (c == '"') {
				c = *p++;
				if (c == '"') {
					push(c);
				} else {
					p--;
					csvtmt_str_clear(buf);
					m = 10;
				}
			} else if (c == sep) {
				store();
				m = 0;
			} else if (c == '\n') {
				goto done;
			} else if (c == '\r') {
				check_crlf();
			} else {
				push(c);
			}
			break;
		}
	}

done:
	store();
	csvtmt_str_del(buf);
	return p;
fail:
	csvtmt_str_del(buf);
	return NULL;
}

int
csvtmt_row_parse_stream(
	CsvTomatoRow *self,
	FILE *fp,
	CsvTomatoError *error
) {
	#undef store
	#define store() {\
		if (any_read) {\
			if (self->len >= csvtmt_numof(self->columns)) {\
				csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "csv line columns overflow");\
				goto fail;\
			}\
			char *s = csvtmt_str_esc_del(buf);\
			self->columns[self->len++] = csvtmt_move(s);\
			buf = csvtmt_str_new();\
			any_read = false;\
		}\
	}\

	#undef push
	#define push(c) {\
		if (!csvtmt_str_push_back(buf, c)) {\
			csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to push back to string buffer");\
			goto fail;\
		}\
	}\

	#undef check_crlf
	#define check_crlf() {\
		c = fgetc(fp);\
		if (c == EOF) {\
			goto done;\
		} else if (c == '\n') {\
			goto done;\
		} else {\
			ungetc(c, fp);\
			goto done;\
		}	\
	}\

	errno = 0;
	CsvTomatoString *buf = csvtmt_str_new();
	if (!buf) {
		csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate string: %s", strerror(errno));
		return 0;
	}

	bool any_read = false;
	int sep = ',';
	int m = 0;
	int ret = 0;

	for (;;) {
		int c = fgetc(fp);
		if (c == EOF) {
			ret = c;
			break;
		}

		switch (m) {
		case 0:
			if (c == '"') {
				m = 10;
				any_read = true;
			} else if (c == sep) {
				any_read = true;
				store();
			} else if (c == '\n') {
				any_read = true;
				goto done;
			} else if (c == '\r') {
				any_read = true;
				check_crlf();
			} else {
				any_read = true;
				push(c);
				m = 30;
			}
			break;
		case 10: // found begin "
			if (c == '"') { 
				c = fgetc(fp);
				if (c == '"') {
					push(c);
				} else {
					ungetc(c, fp);
					m = 20;
				}
			} else {
				push(c);
			}
			break;
		case 20: // found end "
			if (c == sep) {
				store();
				m = 0;
			} else if (c == '\n') {
				goto done;
			} else if (c == '\r') {
				check_crlf();
			} else {
				// ignore
			}
			break;
		case 30: // found normal character
			if (c == '"') {
				c = fgetc(fp);
				if (c == '"') {
					push(c);
				} else {
					ungetc(c, fp);
					csvtmt_str_clear(buf);
					m = 10;
				}
			} else if (c == sep) {
				store();
				m = 0;
			} else if (c == '\n') {
				goto done;
			} else if (c == '\r') {
				check_crlf();
			} else {
				push(c);
			}
			break;
		}
	}

done:
	store();
	csvtmt_str_del(buf);
	return ret;
fail:
	csvtmt_str_del(buf);
	return ret;
}

void
csvtmt_row_final(CsvTomatoRow *self) {
	for (size_t i = 0; i < self->len; i++) {
		free(self->columns[i]);
		self->columns[i] = NULL;
	}
	memset(self, 0, sizeof(*self));
}

static void
append_column_to_stream(
	const char *col,
	FILE *fp,
	bool wrap,
	CsvTomatoError *error
) {
	if (wrap) {
		char *s = csvtmt_wrap_column(col, error);
		if (error->error) {
			return;
		}
		fprintf(fp, "%s", s);
		free(s);
	} else {
		fprintf(fp, "%s", col);
	}
}

void
csvtmt_row_append_to_stream(
	CsvTomatoRow *self,
	FILE *fp,
	bool wrap,
	CsvTomatoError *error
) {
	for (size_t i = 0; i < self->len-1; i++) {
		const char *col = self->columns[i];
		append_column_to_stream(col, fp, wrap, error);
		if (error->error) {
			return;
		}
		fputc(',', fp);
	}	
	if (self->len) {
		const char *col = self->columns[self->len-1];
		append_column_to_stream(col, fp, wrap, error);
		if (error->error) {
			return;
		}
	}
	fputc('\n', fp);
}

void
csvtmt_row_set_clone(
	CsvTomatoRow *self,
	size_t index,
	const char *col,
	CsvTomatoError *error
) {
	if (index >= self->len) {
		csvtmt_error_push(error, CSVTMT_ERR_INDEX_OUT_OF_RANGE, "index out of range");
		return;
	}

	char *clone = csvtmt_strdup(col, error);
	if (error->error) {
		return;
	}

	free(self->columns[index]);
	self->columns[index] = clone;
}
