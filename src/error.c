#include <csvtomato.h>

void _Noreturn
csvtmt_die(const char *s) {
	fprintf(stderr, "%s\n", s);
	fflush(stdout);
	fflush(stderr);
	exit(1);
}

void
csvtmt_error_format(
	CsvTomatoError *self,
	CsvTomatoErrorKind kind,
	const char *fmt,
	...
) {
	self->error = true;
	self->kind = kind;

	va_list args;
	va_start(args, fmt);

	vsnprintf(self->message, sizeof self->message, fmt, args);

	va_end(args);
}

void
csvtmt_error_show(const CsvTomatoError *self) {
	printf("error: %d: %s\n", self->kind, self->message);
	fflush(stdout);
}

const char *
csvtmt_error_msg(const CsvTomatoError *self) {
	return self->message;
}