#include "csvtomato.h"

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