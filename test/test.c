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
	p = csvtmt_parser_new(&error);

	#undef exec
	#define exec(query) {\
		token = csvtmt_tokenizer_tokenize(t, query, &error);\
		node = csvtmt_parser_parse(p, token, &error);\
		assert(!error.error);\
		assert(node);\
		csvtmt_token_del_all(token);\
		csvtmt_node_del_all(node);	\
	}\

	exec(
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");"
	);
	exec(
		"CREATE TABLE users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");"
	);
	exec(
		"INSERT INTO users (id, name) VALUES (1, \"Alice\");"
	);
	exec(
		"INSERT INTO users (id, name) VALUES (1, \"Alice\"), (2, \"Bob\");"
	);
	
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


// ヘルパー：文字列を FILE* に流し込む
FILE *
make_stream(const char *s) {
    FILE *fp = tmpfile();
    if (!fp) return NULL;
    fputs(s, fp);
    rewind(fp);
    return fp;
}

void
parse_stream(const char *input, const char *expected[], int expected_len) {
    CsvTomatoCsvLine line = {0};
    CsvTomatoError error = {0};

    FILE *fp = make_stream(input);
    csvtmt_csvline_parse_stream(&line, fp, &error);
    fclose(fp);

    assert(line.len == expected_len);

    for (int i = 0; i < expected_len; i++) {
        assert(strcmp(line.columns[i], expected[i]) == 0);
    }

	csvtmt_csvline_destroy(&line);
}

void
test_csv(void) {
    // 1. シンプル
    const char *exp1[] = {"a", "b", "c"};
    parse_stream("a,b,c\n", exp1, 3);

    // 2. 空フィールド
    const char *exp2[] = {"a", "", "c"};
    parse_stream("a,,c\n", exp2, 3);

    // 3. クォート付き
    const char *exp3[] = {"a", "hello,world", "c"};
    parse_stream("a,\"hello,world\",c\n", exp3, 3);

    // 4. クォート内のダブルクォートエスケープ
    const char *exp4[] = {"a", "he said \"hi\"", "c"};
    parse_stream("a,\"he said \"\"hi\"\"\",c\n", exp4, 3);

    // 5. 末尾が改行なし
    const char *exp5[] = {"a", "b", "c"};
    parse_stream("a,b,c", exp5, 3);

    // 6. CRLF 改行
    const char *exp6[] = {"a", "b", "c"};
    parse_stream("a,b,c\r\n", exp6, 3);

    // 7. 空行
    const char *exp7[] = {""};
    parse_stream("\n", exp7, 1);

    // 8. クォート内に改行
    const char *exp8[] = {"a", "hello\nworld", "c"};
    parse_stream("a,\"hello\nworld\",c\n", exp8, 3);

    // 9. スペースを含む
    const char *exp9[] = {"a", " b ", "c"};
    parse_stream("a, b ,c\n", exp9, 3);

    // 10. 先頭がクォートで始まる
    const char *exp10[] = {"foo,bar"};
    parse_stream("\"foo,bar\"\n", exp10, 1);

    // 11. 行末空セル
    const char *exp11[] = {"a","b",""};
    parse_stream("a,b,\n", exp11, 3);

    // 12. 複数連続カンマ
    const char *exp12[] = {"a","","","d"};
    parse_stream("a,,,d\n", exp12, 4);

    // 13. 全て空セル
    const char *exp13[] = {"","",""};
    parse_stream(",,\n", exp13, 3);

    // 14. 未閉じクォート（エラー扱い確認）
    const char *exp14[] = {"unterminated\n"};
    parse_stream("\"unterminated\n", exp14, 1); // パーサの仕様に合わせる

    // 15. クォート内にカンマと改行
    const char *exp15[] = {"a,b\nc","d"};
    parse_stream("\"a,b\nc\",d\n", exp15, 2);

    // 16. クォートで囲まれた空セル
    const char *exp16[] = {"a","","c"};
    parse_stream("a,\"\",c\n", exp16, 3);

    // 17. クォートで囲まれたスペース
    const char *exp17[] = {"a"," ","c"};
    parse_stream("a,\" \",c\n", exp17, 3);

    // 18. タブ文字を含むセル
    const char *exp18[] = {"a\tb","c"};
    parse_stream("\"a\tb\",c\n", exp18, 2);

    // 19. 先頭・末尾に空白
    const char *exp19[] = {" a ","b "," c "};
    parse_stream(" a ,b , c \n", exp19, 3);

    // 20. Unicode文字（日本語・絵文字）
    const char *exp20[] = {"太郎","😊","漢字"};
    parse_stream("太郎,😊,漢字\n", exp20, 3);

    // 21. 混合クォートと非クォート
    const char *exp21[] = {"a","b,c","d"};
    parse_stream("a,\"b,c\",d\n", exp21, 3);

    // 22. 列数が不揃い（短い行）
    const char *exp22[] = {"a","b"};
    parse_stream("a,b\n", exp22, 2);

    // 23. 列数が不揃い（長い行）
    const char *exp23[] = {"a","b","c","d"};
    parse_stream("a,b,c,d\n", exp23, 4);

    // 24. クォート内のダブルクォート連続
    const char *exp24[] = {"he said \"hi\"","ok"};
    parse_stream("\"he said \"\"hi\"\"\",ok\n", exp24, 2);

    // 25. 空行（改行だけ）
    const char *exp25[] = {""};
    parse_stream("\n", exp25, 1);

    // 26. 複雑混合（空セル・クォート・改行・カンマ）
    const char *exp26[] = {"a","b,c","d\n e","","f"};
    parse_stream("a,\"b,c\",\"d\n e\",,\"f\"\n", exp26, 5);

    // 27. 末尾改行なし（EOFで終了）
    const char *exp27[] = {"x","y","z"};
    parse_stream("x,y,z", exp27, 3);
}

int 
main(void) {
	// test_tomato();	
	test_csv();
	// test_tokenizer();
	test_parser();
	// test_opcode();
	// test_executor();
	return 0;
}
