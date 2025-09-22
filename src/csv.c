#include <csvtomato.h>

void
csvtmt_csvline_parse_stream(
	CsvTomatoCsvLine *self,
	FILE *fp,
	CsvTomatoError *error
) {
	#define store() {\
		if (any_read) {\
			if (self->len >= csvtmt_numof(self->columns)) {\
				csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "csv line columns overflow");\
				goto fail;\
			}\
			char *s = csvtmt_str_esc_del(buf);\
			self->columns[self->len++] = csvtmt_move(s);\
			buf = csvtmt_str_new();\
			any_read = false;\
		}\
	}\

	#define push(c) {\
		if (!csvtmt_str_push_back(buf, c)) {\
			csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to push back to string buffer");\
			goto fail;\
		}\
	}\

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
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate string: %s", strerror(errno));
		return;
	}

	bool any_read = false;
	int sep = ',';
	int m = 0;

	for (;;) {
		int c = fgetc(fp);
		if (c == EOF) {
			break;
		}

		// printf("m[%d] c[%c]\n", m, c);

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
	return;
fail:
	csvtmt_str_del(buf);
	return;
}

void
csvtmt_csvline_destroy(CsvTomatoCsvLine *self) {
	for (size_t i = 0; i < self->len; i++) {
		free(self->columns[i]);
		self->columns[i] = NULL;
	}
	self->len = 0;
}
