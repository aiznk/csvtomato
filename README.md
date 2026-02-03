# CSV Tomato

![image](./csvtomato.png)

CSVファイルをSQL文で操作できるSQLiteライクなCライブラリです。

## ビルド方法

```
$ make
$ file csvtomato.so
$ ./csvtomato.out
```

## シェルの使い方

`csvtomato.out`を実行するとシェルが起動します。

```
$ mkdir test_db
$ ./csvtomato.out test_db
test_db > 
```

対話モードでSQLを実行できます。

```
test_db > CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, age INTEGER, name TEXT);
test_db > INSERT INTO users (age, name) VALUES (20, "Taro");
test_db > INSERT INTO users (age, name) VALUES (30, "Hanako");
test_db > SELECT * FROM users;
1 20 Taro
2 30 Hanako
```

## ライブラリの使い方

### ヘッダファイルのインクルード

ライブラリを使うには`csvtomato.h`のインクルードが必要です。
このヘッダファイルには構造体の定義などが書かれています。

```c
#include "csvtomato.h"

int main(void) {
	csvtmt_quick_exec("test_db", "SELECT * FROM users");
	return 0;
}
```

ライブラリはまだ開発中なので、構造体のメンバに直接アクセスするのはおすすめしません。
なぜなら破壊的変更が行われる可能性があるからです。

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
	csvtmt_exec(
		db,
		"CREATE TABLE IF NOT EXISTS users ("
		"	id INTEGER PRIMARY KEY AUTOINCREMENT,"
		"	name TEXT NOT NULL,"
		"	age INTEGER"
		");",
		&error
	);
	csvtmt_exec(
		db,
		"INSERT INTO users (name, age) VALUES (\"Alice\", 20);",
		&error
	);
```

`csvtmt_exec()`でSQL文を実行してCSVファイルの操作が可能です。
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
