#include "csvtomato.h"

CsvTomato *
csvtmt_open(
	const char *db_dir,
	CsvTomatoError *error
) {
	errno = 0;
	CsvTomato *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	snprintf(self->db_dir, sizeof self->db_dir, "%s", db_dir);

	return self;
}

void
csvtmt_close(CsvTomato *self) {
	if (!self) {
		return;
	}

	free(self);
}

void
csvtmt_execute(
	CsvTomato *db,
	const char *query,
	CsvTomatoError *error
) {

}

void
csvtmt_prepare(
	CsvTomato *db, 
	const char *query, 
	CsvTomatoStmt **stmt, 
	CsvTomatoError *error
) {

}

void
csvtmt_free(void *ptr) {
	free(ptr);
}

void
csvtmt_static(void *ptr) {
	// pass
}

void
csvtmt_bind_text(
	CsvTomatoStmt *stmt,
	size_t index, 
	const char *text, 
	ssize_t size, 
	void (*destructor)(void*),
	CsvTomatoError *error
) {

}

void
csvtmt_bind_int(
	CsvTomatoStmt *stmt,
	size_t index, 
	ssize_t value, 
	CsvTomatoError *error
) {

}

void
csvtmt_step(CsvTomatoStmt *stmt, CsvTomatoError *error) {

}

void
csvtmt_finalize(CsvTomatoStmt *stmt) {

}
