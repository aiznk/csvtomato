#include <csvtomato.h>

void
csvtmt_error_clear(CsvTomatoError *self) {
	self->error = false;
}

void _Noreturn
csvtmt_die(const char *s) {
	fprintf(stderr, "%s\n", s);
	fflush(stdout);
	fflush(stderr);
	exit(1);
}

void
csvtmt_error_push(
	CsvTomatoError *self,
	CsvTomatoErrorKind kind,
	const char *fmt,
	...
) {
	self->error = true;

	if (self->len >= csvtmt_numof(self->elems)) {
		fprintf(stderr, "csvtomato: error elements overflow\n");
		return;
	}

	CsvTomatoErrorElem *elem = &self->elems[self->len++];

	elem->kind = kind;

	va_list args;
	va_start(args, fmt);

	vsnprintf(elem->message, sizeof elem->message, fmt, args);

	va_end(args);
}

void
csvtmt_error_show(const CsvTomatoError *self) {
	for (size_t i = 0; i < self->len; i++) {
		const CsvTomatoErrorElem *elem = &self->elems[i];
		printf("error: %d: %s\n", elem->kind, elem->message);
	}
	fflush(stdout);
}

const char *
csvtmt_error_msg(const CsvTomatoError *self) {
	if (!self->error) {
		return "nothing error";
	}
	return self->elems[0].message;
}
