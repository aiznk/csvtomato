#include "csvtomato.h"

CsvTomatoStringList *
csvtmt_strlist_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoStringList *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	return self;
}

void
csvtmt_strlist_del(CsvTomatoStringList *self) {
	for (CsvTomatoStringList *cur = self; cur; ) {
		CsvTomatoStringList *rm = cur;
		cur = cur->next;
		free(rm->str);
		free(rm);
	}
}

void
csvtmt_strlist_move_back_str(
	CsvTomatoStringList *self, 
	char *move_str,
	CsvTomatoError *error
) {
	CsvTomatoStringList *elem = csvtmt_strlist_new(error);
	if (error->error) {
		return;
	}

	elem->str = csvtmt_move(move_str);

	for (CsvTomatoStringList *cur = self; cur; cur = cur->next) {
		if (!cur->next) {
			cur->next = elem;
			break;
		}
	}
}
