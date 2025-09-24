# csvtomato

CSVファイルをSQL文で操作できるSQLiteライクなCライブラリです。

## 使い方

### データベースを開く

```c
	CsvTomatoError error = {0};
	CsvTomato *db;
	CsvTomatoStmt *stmt;

	db = csvtmt_open("test_db", &error);
	if (error.error) {
		csvtmt_error_show(&error);
		return;
	}
```

`csvtmt_open()`の第1引数にデータベース名を指定します。
データベースはフォルダです。よって`test_db`というのはフォルダの名前になります。
CSVファイルはこのフォルダ以下に作成されます。

### データベースを閉じる

```c
	csvtmt_close(db);
```

データベースを閉じ忘れるとメモリリークになります。

### クエリを実行する

```c
	csvtmt_execute(
		db,
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		"	age INTEGER"
		");",
		&error
	);
	csvtmt_execute(
		db,
		"INSERT INTO users (name, age) VALUES (\"Alice\", 20);",
		&error
	);
```

`csvtmt_execute()`でSQL文を実行してCSVファイルの操作が可能です。
上記を実行すると以下のようなCSVファイル（`users.csv`）が`test_db`以下に作成されます。

```csv
__MODE__,id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,age INTEGER
0,1,Alice,20
```

### クエリの実行にプレースホルダを使う

```c
	// stmtのメモリを確保
	csvtmt_prepare(
		db,
		"INSERT INTO users (name, age) VALUES (?, ?);",
		&stmt,
		&error
	);

	// プレースホルダに値を紐づける
	csvtmt_bind_text(
		stmt,
		1,
		"Bob",
		-1,
		CSVTMT_TRANSTENT,
		&error
	);
	csvtmt_bind_int(
		stmt,
		2,
		30,
		&error
	);
	
	// 実行
	csvtmt_step(stmt, &error);

	// stmtのメモリを解放
	csvtmt_finalize(stmt);
```

## ライセンス

MIT
