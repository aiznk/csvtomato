#include "csvtomato.h"

void 
test_tomato(void) {
	CsvTomatoError error;
	CsvTomato *db;
	CsvTomatoStmt *stmt;

	db = csvtmt_open("db_dir", &error);
	if (error.error) {
		fprintf(stderr, "%s\n", error.message);
		return;
	}

	csvtmt_execute(
		db,
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		"	age INTEGER"
		")",
		&error
	);

	csvtmt_execute(db, "INSERT INTO users (name, age) VALUES (\"Alice\", 20);", &error);

	csvtmt_prepare(db, "INSERT INTO users (name, age) VALUES (?, ?);", &stmt, &error);

	csvtmt_bind_text(stmt, 1, "Bob", -1, CSVTMT_TRANSTENT, &error);
	csvtmt_bind_int(stmt, 2, 30, &error);

	// 実行
	csvtmt_step(stmt, &error);

	csvtmt_finalize(stmt);
	csvtmt_close(db);
}

int 
main(void) {
	test_tomato();	
	return 0;
}
