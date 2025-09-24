#include <csvtomato.h>

typedef struct {
	const char *key;
	size_t index;
	CsvTomatoValue value;
} ColumnInfo;

typedef struct {
	ColumnInfo array[100];
	size_t len;
} ColumnInfoArray;

typedef struct {
	CsvTomatoCsvLine array[100];
	size_t len;
} CsvLineArray;

static void
store_colinfo(
	CsvTomatoModel *model,
	ColumnInfoArray *infos,
	CsvTomatoKeyValue *kvs,
	size_t kvs_len,
	CsvTomatoError *error
);

static void
value_to_string(CsvTomatoValue *self, char *buf, size_t buf_size, const char **str);

#define impl_static_array(NAMESPACE, STRUCT, TYPE)\
	void\
	NAMESPACE ## _push(STRUCT *self, TYPE elem, CsvTomatoError *error) {\
		if (self->len >= csvtmt_numof(self->array)) {\
			csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "array overflow");\
			return;\
		}\
		self->array[self->len++] = elem;\
	}\

impl_static_array(colinfo_array, ColumnInfoArray, ColumnInfo)
impl_static_array(csvline_array, CsvLineArray, CsvTomatoCsvLine)

void
type_parse_type_def(CsvTomatoColumnType *self, const char *type_def, CsvTomatoError *error) {
	if (strstr(type_def, "INTEGER")) {
		self->type_def_info.integer =  true;
	} else if (strstr(type_def, "TEXT")) {
		self->type_def_info.text = true;
	}
	if (strstr(type_def, "PRIMARY KEY")) {
		self->type_def_info.primary_key = true;
	}
	if (strstr(type_def, "AUTOINCREMENT")) {
		self->type_def_info.autoincrement = true;
	}
	if (strstr(type_def, "NOT NULL")) {
		self->type_def_info.not_null = true;
	} else if (strstr(type_def, "NULL")) {
		self->type_def_info.null = true;
	}
}

// id INTEGER PRIMARY KEY AUTOINCREMENT
// name TEXT NOT NULL
// とかがCSVヘッダカラム。idやnameをtype_nameに、それ以降をtype_defに格納。
void
type_parse_column(
	CsvTomatoColumnType *self,
	const char *col,
	size_t index,
	CsvTomatoError *error
) {
	char *name = self->type_name;
	size_t name_len = 0;
	size_t name_size = sizeof(self->type_name);
	char *def = self->type_def;
	size_t def_len = 0;
	size_t def_size = sizeof(self->type_def);
	int m = 0;

	self->index = index;

	for (const char *p = col; *p; p++) {
		switch (m) {
		case 0:
			if (isspace(*p)) {
				m = 10;
			} else {
				if (name_len >= name_size) {
					goto overflow;
				}
				name[name_len++] = *p;
				name[name_len] = 0;
			}
			break;
		case 10:
			if (isspace(*p)) {
				// pass
			} else {
				if (def_len >= def_size) {
					goto overflow;
				}
				def[def_len++] = *p;
				def[def_len] = 0;
				m = 20;
			}
			break;
		case 20:
			if (def_len >= def_size) {
				goto overflow;
			}
			def[def_len++] = *p;
			def[def_len] = 0;
			m = 20;
			break;
		}
	}

	type_parse_type_def(self, def, error);
	if (error->error) {
		return;
	}

	return;
overflow:
	csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "buffer overflow in parse type column");
}

static void
header_read_from_table(CsvTomatoHeader *self, const char *table_path, CsvTomatoError *error) {
	errno = 0;
	FILE *fp = fopen(table_path, "r");
	if (!fp) {
		csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table: %s: %s", table_path, strerror(errno));
		return;
	}

	CsvTomatoCsvLine row = {0};
	csvtmt_csvline_parse_stream(&row, fp, error);
	if (error->error) {
		fclose(fp);
		return;
	}
	fclose(fp);

	self->types_len = 0;

	for (size_t i = 0; i < row.len; i++) {
		if (self->types_len >= csvtmt_numof(self->types)) {
			goto types_overflow;
		}
		CsvTomatoColumnType *type = &self->types[self->types_len++];
		type->index = i;
		type_parse_column(type, row.columns[i], i, error);
		if (error->error) {
			return;	
		}
		// printf("type: index[%ld] type_name[%s] type_def[%s]\n", type->index, type->type_name, type->type_def);
	}

	csvtmt_csvline_destroy(&row);
	return;
types_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "header types overflow");
	return;
}

static const char *
header_has_column_types(
	CsvTomatoHeader *self, 
	const char *column_names[], 
	size_t column_names_len,
	CsvTomatoError *error
) {
	for (size_t i = 0; i < column_names_len; i++) {
		bool found = false;
		for (size_t j = 0; j < self->types_len; j++) {
			const CsvTomatoColumnType *type = &self->types[j];
			if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
				continue;
			}
			if (!strcmp(column_names[i], type->type_name)) {
				found = true;
				break;
			}
		}
		if (!found) {
			// not found column name in types
			return column_names[i];
		}
	}
	return NULL;
}

static const char *
header_has_key_values_types(
	CsvTomatoHeader *self, 
	CsvTomatoKeyValue key_values[], 
	size_t key_values_len,
	CsvTomatoError *error
) {
	for (size_t i = 0; i < key_values_len; i++) {
		bool found = false;
		for (size_t j = 0; j < self->types_len; j++) {
			const CsvTomatoColumnType *type = &self->types[j];
			if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
				continue;
			}
			if (!strcmp(key_values[i].key, type->type_name)) {
				found = true;
				break;
			}
		}
		if (!found) {
			// not found column name in types
			return key_values[i].key;
		}
	}
	return NULL;
}

static uint64_t
gen_auto_increment_id(
	CsvTomatoModel *model,
	const char *type_name,
	CsvTomatoError *error
) {
	char id_dir[CSVTMT_PATH_SIZE * 2];
	snprintf(id_dir, sizeof id_dir, "%s/id", model->db_dir);
	if (!csvtmt_file_exists(id_dir)) {
		csvtmt_file_mkdir(id_dir);
	}

	char id_path[CSVTMT_PATH_SIZE * 3];
	snprintf(id_path, sizeof id_path, "%s/%s__%s.txt", id_dir, model->table_name, type_name);

	if (!csvtmt_file_exists(id_path)) {
		csvtmt_file_touch(id_path);
	}

	// puts(id_path);
	FILE *id_fp = fopen(id_path, "r+");
	if (!id_fp) {
		goto failed_to_open_file;
	}

	uint64_t id;
	char line[1024];

	fgets(line, sizeof line, id_fp);
	id = atoi(line);
	if (id == 0) {
		id = 1;
	}

	fseek(id_fp, 0, SEEK_SET);
	fprintf(id_fp, "%ld", id+1);

	fclose(id_fp);

	return id;

failed_to_open_file:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open id file: %s", strerror(errno));
	return 0;
}

void
type_gen_column_default_value(
	CsvTomatoModel *model,
	const CsvTomatoColumnType *type,
	char *dst,
	size_t dst_size,
	CsvTomatoError *error
) {
	assert(dst_size >= 2);
	const CsvTomatoColumnTypeDef *info = &type->type_def_info;
	// printf("type_name[%s] type_def[%s]\n", type->type_name, type->type_def);

	if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
		strcpy(dst, "0");
	} else if (info->integer) {
		if (info->autoincrement) {
			uint64_t id = gen_auto_increment_id(model, type->type_name, error);
			if (error->error) {
				goto fail_gen_id;
			}
			snprintf(dst, dst_size, "%ld", id);
		} else {
			strcpy(dst, "0");
		}
	} else if (info->text) {
		strcpy(dst, "");
	} else {
		goto invalid_type_def;
	}

	return;
invalid_type_def:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to generate column default value");
	return;
fail_gen_id:
	return;
}

static void
value_to_string(CsvTomatoValue *self, char *buf, size_t buf_size, const char **str) {
	switch (self->kind) {
	case CSVTMT_VAL_NONE:
		break;
	case CSVTMT_VAL_INT:
		snprintf(buf, buf_size, "%ld", self->int_value);
		break;
	case CSVTMT_VAL_FLOAT:
		snprintf(buf, buf_size, "%f", self->float_value);
		break;
	case CSVTMT_VAL_STRING:
		*str = self->string_value;
		break;
	}
}

static CsvTomatoValue
string_to_value(const char *str) {
	CsvTomatoValue val = {0};
	int m = 0;

	for (const char *p = str; *p; p++) {
		switch (m) {
		case 0:
			if (isdigit(*p)) {
				m = 10;
			} else {
				m = 20;
			}
			break;
		case 10:
			if (*p == '.') {
				m = 30;
			} else if (isdigit(*p)) {
				// pass
			} else {
				m = 20;
			}
			break;
		case 20:
			// pass
			break;
		case 30:
			if (isdigit(*p)) {
				// pass
			} else {
				m = 20;
			}
			break;
		}
	}	
	switch (m) {
	case 10: // int
		val.kind = CSVTMT_VAL_INT;
		val.int_value = atoi(str);	
		break;
	case 20: // string
		val.kind = CSVTMT_VAL_STRING;
		val.string_value = str;
		break;
	case 30: // float
		val.kind = CSVTMT_VAL_FLOAT;
		val.float_value = atof(str);
		break;
	}

	return val;
}

static bool
value_eq(const CsvTomatoValue *lhs, const CsvTomatoValue *rhs) {
	switch (lhs->kind) {
	case CSVTMT_VAL_NONE: return false; break;
	case CSVTMT_VAL_INT:
		switch (rhs->kind) {
		default: return false; break;
		case CSVTMT_VAL_INT:
			return lhs->int_value == rhs->int_value;
			break;
		case CSVTMT_VAL_FLOAT:
			return lhs->int_value == rhs->float_value;
			break;
		}
		break;
	case CSVTMT_VAL_FLOAT:
		switch (rhs->kind) {
		default: return false; break;
		case CSVTMT_VAL_INT:
			return lhs->float_value == rhs->int_value;
			break;
		case CSVTMT_VAL_FLOAT:
			return lhs->float_value == rhs->float_value;
			break;
		}
		break;
	case CSVTMT_VAL_STRING:
		switch (rhs->kind) {
		default: return false; break;
		case CSVTMT_VAL_STRING:
			return !strcmp(lhs->string_value, rhs->string_value);
			break;
		}
		break;
	}

	return false;
}

void
append_csv_lines(CsvTomatoModel *model, CsvLineArray *lines, CsvTomatoError *error) {
	errno = 0;
	FILE *fp = fopen(model->table_path, "a");
	if (!fp) {
		goto failed_to_open_table;
	}

	for (size_t i = 0; i < lines->len; i++) {
		CsvTomatoCsvLine *line = &lines->array[i];
		csvtmt_csvline_append_to_stream(line, fp, error);
		if (error->error) {
			goto failed_to_append;
		}	
	}

	fclose(fp);
	return;
failed_to_append:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to append lines to stream");
	fclose(fp);
	return;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table: %s: %s", model->table_path, strerror(errno));
	return;
}

static void
mkdir_tmp_dir(const char *db_dir, char *tmp_dir, size_t tmp_dir_size) {
	snprintf(tmp_dir, tmp_dir_size, "%s/tmp", db_dir);
	if (!csvtmt_file_exists(tmp_dir)) {
		csvtmt_file_mkdir(tmp_dir);
	}
}

static void
replace_update(CsvTomatoModel *model, CsvTomatoError *error) {
	FILE *fin, *fout;
	ColumnInfoArray infos = {0};

	store_colinfo(model, &infos, model->update_set_key_values, model->update_set_key_values_len, error);
	if (error->error) {
		return;
	}

	errno = 0;
	fin = fopen(model->table_path, "r");
	if (!fin) {
		goto failed_to_open_table;
	}

	char tmp_dir[CSVTMT_PATH_SIZE + 10];
	mkdir_tmp_dir(model->db_dir, tmp_dir, sizeof tmp_dir);

	char tmp_path[CSVTMT_PATH_SIZE * 2 + 10];
	snprintf(tmp_path, sizeof tmp_path, "%s/tmp.csv", tmp_dir);

	fout = fopen(tmp_path, "w");
	if (!fout) {
		goto failed_to_open_tmp_file;
	}

	CsvTomatoCsvLine row = {0};
	size_t nline = 0;

	for (;; nline++) {
		int ret = csvtmt_csvline_parse_stream(&row, fin, error);
		if (error->error) {
			goto failed_to_parse_stream;
		}
		if (ret == EOF) {
			break;
		}

		if (nline != 0) {
			for (size_t ii = 0; ii < infos.len; ii++) {
				ColumnInfo *info = &infos.array[ii];

				for (size_t ci = 0; ci < row.len; ci++) {
					if (info->index == ci) {
						const char *str = NULL;
						char buf[CSVTMT_STR_SIZE];
						value_to_string(&info->value, buf, sizeof buf, &str);
						csvtmt_csvline_set_clone(&row, info->index, buf, error);
						if (error->error) {
							goto failed_to_replace;
						}		
					}
				}
			}
		}

		csvtmt_csvline_append_to_stream(&row, fout, error);
		if (error->error) {
			goto failed_to_append_to_stream;
		}

		csvtmt_csvline_destroy(&row);
	}

	if (csvtmt_file_rename(tmp_path, model->table_path) == -1) {
		goto failed_to_rename_csv_file;
	}

	fclose(fin);
	fclose(fout);
	return;
failed_to_rename_csv_file:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to rename csv file: %s", strerror(errno));
	return;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table: %s: %s", model->table_path, strerror(errno));
	return;
failed_to_open_tmp_file:
	fclose(fin);
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open tmp file: %s: %s", tmp_path, strerror(errno));
	return;
failed_to_parse_stream:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse stream");
	return;
failed_to_append_to_stream:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to append to stream");
	return;
failed_to_replace:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to replace column");
	return;
}

static void
store_colinfo(
	CsvTomatoModel *model,
	ColumnInfoArray *infos,
	CsvTomatoKeyValue *kvs,
	size_t kvs_len,
	CsvTomatoError *error
) {
	// ヘッダのタイプ列にkvsのkeyがあるか見る。
	// あれば、そのタイプのインデックスを得る。
	// このインデックスがWHERE比較をする列番号になる。
	CsvTomatoColumnType *types = model->header.types;
	size_t types_len = model->header.types_len;

	for (size_t ti = 0; ti < types_len; ti++) {
		CsvTomatoColumnType *type = &types[ti];
		for (size_t kvi = 0; kvi < kvs_len; kvi++) {
			CsvTomatoKeyValue *kv = &kvs[kvi];
			if (!strcmp(type->type_name, kv->key)) {
				ColumnInfo info = {
					.key = type->type_name,
					.index = ti,
					.value = kv->value,
				};
				colinfo_array_push(infos, info, error);
				if (error->error) {
					goto array_overflow;
				}
				break;
			}
		}
	}

	return;
array_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "array overflow");
	return;
}

static void
replace_row(
	CsvTomatoModel *model,
	CsvTomatoCsvLine *row,
	ColumnInfoArray *infos, 
	CsvTomatoError *error
) {
	for (size_t i = 0; i < infos->len; i++) {
		ColumnInfo *info = &infos->array[i];
		char buf[1024];
		const char *str = NULL;
		value_to_string(&info->value, buf, sizeof buf, &str);
		const char *p = str ? str : buf;
		// printf("replace value[%s]\n", p);
		csvtmt_csvline_set_clone(row, info->index, p, error);
	}
}

void
csvtmt_update(CsvTomatoModel *model, CsvTomatoError *error) {
	const char *not_found = NULL;
	bool has_where = model->where_key_values_len;
	
	header_read_from_table(&model->header, model->table_path, error);
	if (error->error) {
		goto failed_to_header_read;
	
	}

	not_found = header_has_key_values_types(
		&model->header,
		model->update_set_key_values,
		model->update_set_key_values_len,
		error
	);
	if (not_found) {
		goto invalid_key_values_type;
	}

	if (has_where) {
		// WHEREのあるUPDATEでは該当行の論理削除と該当行をコピーしてファイルへの追記を行う。
		
		// UPDATE table 
		// 		SET age = 21, name = "hoge"
		// 		WHERE age = 20

		// SETのageとnameは順不同。
		// ヘッダのカラムからageとnameの列インデックスを得る必要がある。

		char *ptr;
		int fd;
		size_t size;

		{ // Linux
			errno = 0;
			fd = open(model->table_path, O_RDWR);
			if (fd == -1) {
				goto failed_to_open_table;
			}

			struct stat st;
			errno = 0;
			if (fstat(fd, &st) == -1) {
				close(fd);
				goto failed_to_stat;
			}

			size = st.st_size;

			ptr = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
			if (ptr == MAP_FAILED) {
				goto failed_to_mmap;
			}
		}

		ColumnInfoArray where_infos = {0};
		ColumnInfoArray set_infos = {0};

		store_colinfo(model, &where_infos, model->where_key_values, model->where_key_values_len, error);
		if (error->error) {
			goto array_overflow;
		}
		store_colinfo(model, &set_infos, model->update_set_key_values, model->update_set_key_values_len, error);
		if (error->error) {
			goto array_overflow;
		}

		CsvTomatoCsvLine row = {0};
		CsvLineArray lines = {0};
		char *p = ptr;

		// skip first line (header)
		p = (char *) csvtmt_csvline_parse_string(&row, p, error);
		if (error->error) {
			goto failed_to_parse_csv;
		}

		// CSVファイルを走査して、indicesの列の値がkvsのvalueと
		// 一致しているか見る。一致していればWHEREにマッチしている行と見なす。
		// WHERE value1 = 123
		for (; *p; ) {
			char *head = p;
			p = (char *) csvtmt_csvline_parse_string(&row, p, error);
			if (error->error) {
				goto failed_to_parse_csv;
			}
			if (!strcmp(row.columns[0], "1")) {
				csvtmt_csvline_destroy(&row);
				continue;  // this line deleted
			}

			for (size_t ii = 0; ii < where_infos.len; ii++) {
				ColumnInfo *winfo = &where_infos.array[ii];

				for (size_t ci = 0; ci < row.len; ci++) {
					const char *col = row.columns[ci];
					if (ci == winfo->index) {
						CsvTomatoValue val = string_to_value(col);
						if (value_eq(&winfo->value, &val)) {
							// match line 
							// printf("match: %s\n", col);

							// replace
							replace_row(model, &row, &set_infos, error);
							csvline_array_push(&lines, row, error);
							if (error->error) {
								goto array_overflow;
							}
							memset(&row, 0, sizeof(row));

							// mode to delete 
							if (*head == '"') {
								head++;
							}
							*head = '1';  // __MODE__ column to 1 (delete)
							// printf("head[%s]\n", head);
						}
					}
				}
			}

			csvtmt_csvline_destroy(&row);
		}

		append_csv_lines(model, &lines, error);
		if (error->error) {
			goto failed_to_append_lines;
		}

		for (size_t i = 0; i < lines.len; i++) {
			csvtmt_csvline_destroy(&lines.array[i]);
		}

		{ // Linux
			munmap(ptr, size);
			close(fd);
		}
	} else {
		// UPDATE table SET age = 123, name = "Taro"
		// WHERE句がない。全置換。
		replace_update(model, error);
		if (error->error) {
			goto failed_to_replace_update;
		}
	}

	return;

failed_to_header_read:
	return;
failed_to_replace_update:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to replace update");
	return;
invalid_key_values_type:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid update key. \"%s\" is not in header types", not_found);
	return;
failed_to_stat:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to get table size: %s", strerror(errno));
	return;
failed_to_mmap:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to mmap. %s", strerror(errno));
	return;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return;
failed_to_parse_csv:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse csv line");
	return;
array_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "array overflow");
	return;
failed_to_append_lines:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to append csv lines");
	return;
}

void
csvtmt_insert(CsvTomatoModel *model, CsvTomatoError *error) {
	// INSERTでは単純なファイルへの追記を行う。
	const char *not_found = NULL;
	
	header_read_from_table(&model->header, model->table_path, error);
	if (error->error) {
		goto failed_to_header_read;
	}

	// INSERT INTO table (id, name) VALUES (1, "Alice")
	// だった場合はヘッダにid, nameが有るか調べる。
	// そのインデックスの位置にVALUESをセットする。
	not_found = header_has_column_types(
		&model->header,
		model->column_names,
		model->column_names_len,
		error
	);
	if (not_found) {
		goto invalid_column;
	}

	// open table
	errno = 0;
	FILE *fp = fopen(model->table_path, "a");
	if (!fp) {
		goto failed_to_open_table;
	}

	// valuesは二次元配列。
	// VALUES (1, 2), (3, 4), (5, 6)
	// などに対応する。
	for (size_t i = 0; i < model->values_len; i++) {
		CsvTomatoValues *values = &model->values[i];

		if (values->len != model->column_names_len) {
			// printf("%ld %ld %ld\n", model->values_len, values->len, model->column_names_len);
			goto invalid_values_len;
		}

		csvtmt_str_clear(model->buf);

		// types:t1,t2,t3
		// column_names:t1,t3
		// values:1,3
		for (size_t j = 0; j < model->header.types_len; j++) {
			const CsvTomatoColumnType *type = &model->header.types[j];
			if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
				csvtmt_str_append(model->buf, "0,");
				continue;
			} 

			// INSERT INTO table (id, name) VALUES (0, "Alice")
			// INSERT INTO table (id, name) VALUES (0, "Alice")

			int values_index = -1;
			for (size_t k = 0; k < model->column_names_len; k++) {
				const char *column_name = model->column_names[k];
				if (!strcmp(type->type_name, column_name)) {
					values_index = k;
					break;
				}
			}
			if (values_index == -1) {
				// j は (id, name) などで指定されていないカラム。
				// typesの情報を元にデフォルト値を入れる。
				char col[1024];
				type_gen_column_default_value(model, type, col, sizeof col, error);
				if (error->error) {
					goto failed_to_gen_type_string;
				}
				csvtmt_str_append(model->buf, col);
			} else {
				// values_indexはカラムに含まれている。
				if (values_index >= values->len) {
					goto values_index_out_of_ragen;
				}
				const CsvTomatoValue *value = &values->values[values_index];
				char buf[1024];
				// printf("value %d\n", value->kind);

				switch (value->kind) {
				default: goto invalid_value_kind; break;
				case CSVTMT_VAL_INT:
					snprintf(buf, sizeof buf, "%ld", value->int_value);
					csvtmt_str_append(model->buf, buf);
					break;
				case CSVTMT_VAL_FLOAT:
					snprintf(buf, sizeof buf, "%f", value->float_value);
					csvtmt_str_append(model->buf, buf);
					break;
				case CSVTMT_VAL_STRING:
					// TODO: wrap by double-quote
					assert(value->string_value);
					csvtmt_str_append(model->buf, value->string_value);
					break;
				}
			}

			csvtmt_str_push_back(model->buf, ',');
		}

		csvtmt_str_pop_back(model->buf); // ,
		fprintf(fp, "%s\n", model->buf->str);
	}

	fclose(fp);
	return;

failed_to_gen_type_string:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to generate type column value");
	return;
failed_to_header_read:
	return;
invalid_column:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid column name. \"%s\" is not in header types", not_found);
	return;
invalid_values_len:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid values length. column names len is \"%ld\" but values length is \"%ld\"", model->column_names_len, model->values_len);
	return;
invalid_value_kind:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid value kind");
	return;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return;
values_index_out_of_ragen:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "values index out of range");
	return;
}