#include "../csvtomato.h"
#include <assert.h>

void 
test_tomato(void) {
	CsvTomatoError error = {0};
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

void
test_tokenizer(void) {
	CsvTomatoError error = {0};
	CsvTomatoTokenizer *t;
	CsvTomatoToken *token;

	t = csvtmt_tokenizer_new(&error);
	assert(t);
	assert(!error.error);

	token = csvtmt_tokenizer_tokenize(
		t, 
		"INSERT INTO users (id, name) VALUES (?, ?);", 
		&error
	);

	csvtmt_tokenizer_del(t);
	csvtmt_token_del_all(token);
}

void
test_parser(void) {
	CsvTomatoError error = {0};
	CsvTomatoTokenizer *t;
	CsvTomatoParser *p;
	CsvTomatoToken *token;
	CsvTomatoNode *node;

	t = csvtmt_tokenizer_new(&error);
	token = csvtmt_tokenizer_tokenize(
		t, 
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");",
		&error
	);

	p = csvtmt_parser_new(&error);
	assert(!error.error);
	assert(p);

	node = csvtmt_parser_parse(p, token, &error);
	assert(!error.error);
	assert(node);

	csvtmt_token_del_all(token);
	csvtmt_node_del_all(node);
	csvtmt_parser_del(p);
	csvtmt_tokenizer_del(t);
}

void
test_opcode(void) {
	CsvTomatoError error = {0};
	CsvTomatoTokenizer *t;
	CsvTomatoParser *p;
	CsvTomatoOpcode *o;
	CsvTomatoToken *token;
	CsvTomatoNode *node;

	t = csvtmt_tokenizer_new(&error);
	token = csvtmt_tokenizer_tokenize(
		t, 
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");",
		&error
	);

	p = csvtmt_parser_new(&error);
	assert(!error.error);
	assert(p);

	node = csvtmt_parser_parse(p, token, &error);
	assert(!error.error);
	assert(node);

	o = csvtmt_opcode_new(&error);
	assert(!error.error);
	assert(o);

	csvtmt_opcode_parse(o, node, &error);
	assert(!error.error);

	csvtmt_token_del_all(token);
	csvtmt_node_del_all(node);
	csvtmt_parser_del(p);
	csvtmt_tokenizer_del(t);
	csvtmt_opcode_del(o);
}

void
test_executor(void) {
	CsvTomatoError error = {0};
	CsvTomatoTokenizer *t;
	CsvTomatoParser *p;
	CsvTomatoExecutor *e;
	CsvTomatoOpcode *o;
	CsvTomatoToken *token;
	CsvTomatoNode *node;

	const char *query = "CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");";

	t = csvtmt_tokenizer_new(&error);
	token = csvtmt_tokenizer_tokenize(t, query, &error);

	p = csvtmt_parser_new(&error);
	assert(!error.error);
	assert(p);

	node = csvtmt_parser_parse(p, token, &error);
	assert(!error.error);
	assert(node);

	o = csvtmt_opcode_new(&error);
	assert(!error.error);
	assert(o);

	csvtmt_opcode_parse(o, node, &error);
	assert(!error.error);

	if (!csvtmt_file_exists("test_db")) {
		csvtmt_file_mkdir("test_db");
	}

	e = csvtmt_executor_new("test_db", &error);
	assert(!error.error);
	assert(e);

	csvtmt_executor_exec(e, o->elems, o->len, &error);
	assert(!error.error);

	csvtmt_token_del_all(token);
	csvtmt_node_del_all(node);
	csvtmt_parser_del(p);
	csvtmt_tokenizer_del(t);
	csvtmt_opcode_del(o);
	csvtmt_executor_del(e);
}

int 
main(void) {
	// test_tomato();	
	// test_tokenizer();
	// test_parser();
	// test_opcode();
	test_executor();
	return 0;
}
