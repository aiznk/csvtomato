#include "csvtomato.h"

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