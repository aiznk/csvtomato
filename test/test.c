#include "../csvtomato.h"
#include <assert.h>

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

void parse_string(const char *input, const char *expected[], int expected_len) {
    CsvTomatoCsvLine line = {0};
    CsvTomatoError error = {0};

    csvtmt_csvline_parse_string(&line, input, &error);

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
    parse_string("a,b,c\n", exp1, 3);

    // 2. 空フィールド
    const char *exp2[] = {"a", "", "c"};
    parse_stream("a,,c\n", exp2, 3);
    parse_string("a,,c\n", exp2, 3);
	
    // 3. クォート付き
    const char *exp3[] = {"a", "hello,world", "c"};
    parse_stream("a,\"hello,world\",c\n", exp3, 3);
    parse_string("a,\"hello,world\",c\n", exp3, 3);
	
    // 4. クォート内のダブルクォートエスケープ
    const char *exp4[] = {"a", "he said \"hi\"", "c"};
    parse_stream("a,\"he said \"\"hi\"\"\",c\n", exp4, 3);
    parse_string("a,\"he said \"\"hi\"\"\",c\n", exp4, 3);
	
    // 5. 末尾が改行なし
    const char *exp5[] = {"a", "b", "c"};
    parse_stream("a,b,c", exp5, 3);
    parse_string("a,b,c", exp5, 3);
	
    // 6. CRLF 改行
    const char *exp6[] = {"a", "b", "c"};
    parse_stream("a,b,c\r\n", exp6, 3);
    parse_string("a,b,c\r\n", exp6, 3);
	
    // 7. 空行
    const char *exp7[] = {""};
    parse_stream("\n", exp7, 1);
    parse_string("\n", exp7, 1);
	
    // 8. クォート内に改行
    const char *exp8[] = {"a", "hello\nworld", "c"};
    parse_stream("a,\"hello\nworld\",c\n", exp8, 3);
    parse_string("a,\"hello\nworld\",c\n", exp8, 3);
	
    // 9. スペースを含む
    const char *exp9[] = {"a", " b ", "c"};
    parse_stream("a, b ,c\n", exp9, 3);
    parse_string("a, b ,c\n", exp9, 3);
	
    // 10. 先頭がクォートで始まる
    const char *exp10[] = {"foo,bar"};
    parse_stream("\"foo,bar\"\n", exp10, 1);
    parse_string("\"foo,bar\"\n", exp10, 1);
	
    // 11. 行末空セル
    const char *exp11[] = {"a","b",""};
    parse_stream("a,b,\n", exp11, 3);
    parse_string("a,b,\n", exp11, 3);
	
    // 12. 複数連続カンマ
    const char *exp12[] = {"a","","","d"};
    parse_stream("a,,,d\n", exp12, 4);
    parse_string("a,,,d\n", exp12, 4);
	
    // 13. 全て空セル
    const char *exp13[] = {"","",""};
    parse_stream(",,\n", exp13, 3);
    parse_string(",,\n", exp13, 3);
	
    // 14. 未閉じクォート（エラー扱い確認）
    const char *exp14[] = {"unterminated\n"};
    parse_stream("\"unterminated\n", exp14, 1); // パーサの仕様に合わせる
    parse_stream("\"unterminated\n", exp14, 1); // パーサの仕string
	
    // 15. クォート内にカンマと改行
    const char *exp15[] = {"a,b\nc","d"};
    parse_stream("\"a,b\nc\",d\n", exp15, 2);
    parse_string("\"a,b\nc\",d\n", exp15, 2);
	
    // 16. クォートで囲まれた空セル
    const char *exp16[] = {"a","","c"};
    parse_stream("a,\"\",c\n", exp16, 3);
    parse_string("a,\"\",c\n", exp16, 3);
	
    // 17. クォートで囲まれたスペース
    const char *exp17[] = {"a"," ","c"};
    parse_stream("a,\" \",c\n", exp17, 3);
    parse_string("a,\" \",c\n", exp17, 3);
	
    // 18. タブ文字を含むセル
    const char *exp18[] = {"a\tb","c"};
    parse_stream("\"a\tb\",c\n", exp18, 2);
    parse_string("\"a\tb\",c\n", exp18, 2);
	
    // 19. 先頭・末尾に空白
    const char *exp19[] = {" a ","b "," c "};
    parse_stream(" a ,b , c \n", exp19, 3);
    parse_string(" a ,b , c \n", exp19, 3);
	
    // 20. Unicode文字（日本語・絵文字）
    const char *exp20[] = {"太郎","😊","漢字"};
    parse_stream("太郎,😊,漢字\n", exp20, 3);
    parse_string("太郎,😊,漢字\n", exp20, 3);
	
    // 21. 混合クォートと非クォート
    const char *exp21[] = {"a","b,c","d"};
    parse_stream("a,\"b,c\",d\n", exp21, 3);
    parse_string("a,\"b,c\",d\n", exp21, 3);
	
    // 22. 列数が不揃い（短い行）
    const char *exp22[] = {"a","b"};
    parse_stream("a,b\n", exp22, 2);
    parse_string("a,b\n", exp22, 2);
	
    // 23. 列数が不揃い（長い行）
    const char *exp23[] = {"a","b","c","d"};
    parse_stream("a,b,c,d\n", exp23, 4);
    parse_string("a,b,c,d\n", exp23, 4);
	
    // 24. クォート内のダブルクォート連続
    const char *exp24[] = {"he said \"hi\"","ok"};
    parse_stream("\"he said \"\"hi\"\"\",ok\n", exp24, 2);
    parse_string("\"he said \"\"hi\"\"\",ok\n", exp24, 2);
	
    // 25. 空行（改行だけ）
    const char *exp25[] = {""};
    parse_stream("\n", exp25, 1);
    parse_string("\n", exp25, 1);
	
    // 26. 複雑混合（空セル・クォート・改行・カンマ）
    const char *exp26[] = {"a","b,c","d\n e","","f"};
    parse_stream("a,\"b,c\",\"d\n e\",,\"f\"\n", exp26, 5);
    parse_string("a,\"b,c\",\"d\n e\",,\"f\"\n", exp26, 5);
	
    // 27. 末尾改行なし（EOFで終了）
    const char *exp27[] = {"x","y","z"};
    parse_stream("x,y,z", exp27, 3);
    parse_string("x,y,z", exp27, 3);
	}

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
	CsvTomatoModel model = {0};

	#undef die
	#define die() {\
		if (error.error) {\
			csvtmt_error_show(&error);\
			exit(1);\
		}\
	}\

	#undef setup
	#define setup() {\
		t = csvtmt_tokenizer_new(&error);\
		p = csvtmt_parser_new(&error);\
		o = csvtmt_opcode_new(&error);\
		e = csvtmt_executor_new(&error);\
	}\

	#undef cleanup
	#define cleanup() {\
		csvtmt_token_del_all(token);\
		csvtmt_node_del_all(node);\
		csvtmt_parser_del(p);\
		csvtmt_tokenizer_del(t);\
		csvtmt_opcode_del(o);\
		csvtmt_executor_del(e);\
	}\

	#undef clear
	#define clear(table_name) {\
		csvtmt_file_remove("test_db/" table_name ".csv");\
		csvtmt_file_remove("test_db/id/" table_name "__id.txt");\
	}\

	#undef exec
	#define exec(query, hope) {\
		setup();\
		token = csvtmt_tokenizer_tokenize(t, query, &error);\
		die();\
		node = csvtmt_parser_parse(p, token, &error);\
		die();\
		csvtmt_opcode_parse(o, node, &error);\
		die();\
		csvtmt_executor_exec(e, &model, "test_db", o->elems, o->len, &error);\
		die();\
		FILE *fp = fopen(model.table_path, "r");\
		char buf[1024];\
		size_t n = fread(buf, sizeof(buf[0]), 1024-1, fp);\
		buf[n] = 0;\
		if (strcmp(buf, hope)) {\
			printf("buf[%s]\n", buf);\
			assert(!strcmp(buf, hope));\
		}\
		fclose(fp);\
		cleanup();\
	}\

	#undef exec_fail
	#define exec_fail(query, errmsg) {\
		token = csvtmt_tokenizer_tokenize(t, query, &error);\
		die();\
		node = csvtmt_parser_parse(p, token, &error);\
		die();\
		csvtmt_opcode_parse(o, node, &error);\
		die();\
		csvtmt_executor_exec(e, &model, "test_db", o->elems, o->len, &error);\
		if (error.error) {\
			csvtmt_error_show(&error);\
			assert(strstr(csvtmt_error_msg(&error), errmsg));\
			exit(1);\
		} else {\
			assert(0 && "failed exec_fail. no errors");\
		}\
	}\

	if (!csvtmt_file_exists("test_db")) {
		csvtmt_file_mkdir("test_db");
	}

	clear("users");
	exec(
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		" 	age INTEGER"
		");",
		"__MODE__,id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,age INTEGER\n"
	);
	// exec_fail(
	// 	"CREATE TABLE users ("
	// 	"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
	// 	"	name TEXT NOT NULL,"
	// 	" 	age INTEGER"
	// 	");",
	// 	"table users already exists"
	// );
	exec(
		"INSERT INTO users (name, age) VALUES (\"Alice\", 20);",
		"__MODE__,id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,age INTEGER\n"
		"0,1,Alice,20\n"
	);
	exec(
		"INSERT INTO users (name, age) VALUES (\"Hanako\", 123), (\"Taro\", 223)",
		"__MODE__,id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,age INTEGER\n"
		"0,1,Alice,20\n"
		"0,2,Hanako,123\n"
		"0,3,Taro,223\n"
	);
	exec(
		"UPDATE users SET age = 200, name = \"Tamako\" WHERE id = 2;",
		"__MODE__,id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,age INTEGER\n"
		"0,1,Alice,20\n"
		"1,2,Hanako,123\n"
		"0,3,Taro,223\n"		
		"0,2,Tamako,200\n"
	);
	// exec(
	// 	"UPDATE users SET age = 123, id = 3",
	// );
	// exec(
	// 	"INSERT INTO users (id, name, age) VALUES (1, \"Alice\", 20);"
	// );
}


int 
main(void) {
	// test_tomato();	
	// test_csv();
	// test_tokenizer();
	// test_parser();
	// test_opcode();
	test_executor();
	return 0;
}
