#include "csvtomato.h"

CsvTomato *
csvtmt_open(
	const char *db_dir,
	CsvTomatoError *error
) {
	errno = 0;
	CsvTomato *db = calloc(1, sizeof(*tmp));
	if (!db) {
		error->error = true;
		error->kind = CSVTMT_ERR_MEM;
		snprintf(error->message, sizeof error->message, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	snprintf(db->db_dir, sizeof db->db_dir, "%s", db_dir);

	return db;
}

void
csvtmt_close(CsvTomato *db) {
	if (!db) {
		return;
	}

	free(db);
}