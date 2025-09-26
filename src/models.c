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

static char *
value_to_column(
	const CsvTomatoValue *self,
	bool wrap,
	CsvTomatoError *error
);

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
csvtmt_model_init(
	CsvTomatoModel *self,
	const char *db_dir,
	CsvTomatoError *error
) {
	memset(self, 0, sizeof(*self));
	snprintf(self->db_dir, sizeof self->db_dir, "%s", db_dir);
}

void
csvtmt_model_final(CsvTomatoModel *self) {
	csvtmt_csvline_final(&self->row);
	if (self->mmap.fd) {
		munmap(self->mmap.ptr, self->mmap.size);
		close(self->mmap.fd);
		self->mmap.ptr = self->mmap.cur = NULL;
		self->mmap.fd = 0;
	}
}

static void
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
static void
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

	csvtmt_csvline_final(&row);
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

static char *
value_to_column(
	const CsvTomatoValue *self,
	bool wrap,
	CsvTomatoError *error
) {
	char buf[CSVTMT_STR_SIZE];

	switch (self->kind) {
	case CSVTMT_VAL_PLACE_HOLDER:
	case CSVTMT_VAL_NONE: break;
	case CSVTMT_VAL_INT:
		snprintf(buf, sizeof buf, "%ld", self->int_value);
		return csvtmt_strdup(buf, error);
		break;
	case CSVTMT_VAL_DOUBLE:
		snprintf(buf, sizeof buf, "%f", self->double_value);
		return csvtmt_strdup(buf, error);
		break;
	case CSVTMT_VAL_STRING:
		if (wrap) {
			return csvtmt_wrap_column(self->string_value, error);
		} else {
			return csvtmt_strdup(self->string_value, error);
		}
		break;
	}

	return NULL;
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
	case 30: // double
		val.kind = CSVTMT_VAL_DOUBLE;
		val.double_value = atof(str);
		break;
	}

	return val;
}

static bool
value_eq(const CsvTomatoValue *lhs, const CsvTomatoValue *rhs) {
	switch (lhs->kind) {
	case CSVTMT_VAL_NONE: 
	case CSVTMT_VAL_PLACE_HOLDER: return false; break;
	case CSVTMT_VAL_INT:
		switch (rhs->kind) {
		default: return false; break;
		case CSVTMT_VAL_INT:
			return lhs->int_value == rhs->int_value;
			break;
		case CSVTMT_VAL_DOUBLE:
			return lhs->int_value == rhs->double_value;
			break;
		}
		break;
	case CSVTMT_VAL_DOUBLE:
		switch (rhs->kind) {
		default: return false; break;
		case CSVTMT_VAL_INT:
			return lhs->double_value == rhs->int_value;
			break;
		case CSVTMT_VAL_DOUBLE:
			return lhs->double_value == rhs->double_value;
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

static void
append_csv_lines(
	CsvTomatoModel *model,
	CsvLineArray *lines,
	bool wrap,
	CsvTomatoError *error
) {
	errno = 0;
	FILE *fp = fopen(model->table_path, "a");
	if (!fp) {
		goto failed_to_open_table;
	}

	for (size_t i = 0; i < lines->len; i++) {
		CsvTomatoCsvLine *line = &lines->array[i];
		csvtmt_csvline_append_to_stream(line, fp, wrap, error);
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

static int
update_all(CsvTomatoModel *model, CsvTomatoError *error) {
	FILE *fin, *fout;
	ColumnInfoArray infos = {0};

	store_colinfo(model, &infos, model->update_set_key_values, model->update_set_key_values_len, error);
	if (error->error) {
		return CSVTMT_ERROR;
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
		csvtmt_csvline_final(&row);
		int ret = csvtmt_csvline_parse_stream(&row, fin, error);
		if (error->error) {
			goto failed_to_parse_stream;
		}
		if (ret == EOF) {
			break;
		}

		// csvtmt_csvline_show(&row);
		if (row.len && !strcmp(row.columns[0], "1")) {
			continue; // this row deleted
		}

		if (nline != 0) {
			for (size_t ii = 0; ii < infos.len; ii++) {
				ColumnInfo *info = &infos.array[ii];

				for (size_t ci = 0; ci < row.len; ci++) {
					if (info->index == ci) {
						char *s = value_to_column(&info->value, false, error);
						if (error->error) {
							goto failed_to_value_to_string;
						}
						csvtmt_csvline_set_clone(&row, info->index, s, error);
						free(s);
						if (error->error) {
							goto failed_to_replace;
						}		
					}
				}
			}
		}

		csvtmt_csvline_append_to_stream(&row, fout, true, error);
		if (error->error) {
			goto failed_to_append_to_stream;
		}

		csvtmt_csvline_final(&row);
	}

	if (csvtmt_file_rename(tmp_path, model->table_path) == -1) {
		goto failed_to_rename_csv_file;
	}

	fclose(fin);
	fclose(fout);
	return CSVTMT_OK;

failed_to_value_to_string:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to value to string");
	return CSVTMT_ERROR;
failed_to_rename_csv_file:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to rename csv file");
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table: %s: %s", model->table_path, strerror(errno));
	return CSVTMT_ERROR;
failed_to_open_tmp_file:
	fclose(fin);
	csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open tmp file: %s: %s", tmp_path, strerror(errno));
	return CSVTMT_ERROR;
failed_to_parse_stream:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse stream");
	return CSVTMT_ERROR;
failed_to_append_to_stream:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to append to stream");
	return CSVTMT_ERROR;
failed_to_replace:
	fclose(fin);
	fclose(fout);
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to replace column");
	return CSVTMT_ERROR;
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
store_selected_columns(CsvTomatoModel *model, CsvTomatoCsvLine *row, CsvTomatoError *error) {
	CsvTomatoColumnType *types = model->header.types;
	size_t tlen = model->header.types_len;
	const char **cols = model->column_names;
	const char *col;
	size_t clen = model->column_names_len;
	CsvTomatoColumnType *type;

	if (tlen != row->len) {
		csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid row length");
		return;
	}

	model->selected_columns_len = 0;

	for (size_t ti = 0; ti < tlen; ti++) {
		type = &types[ti];
		for (size_t ci = 0; ci < clen; ci++) {
			col = cols[ci];
			if (!strcmp(type->type_name, col)) {
				if (model->selected_columns_len >= csvtmt_numof(model->selected_columns)) {
					csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "selected columns overflow");
					return;
				}
				model->selected_columns[model->selected_columns_len++] = row->columns[ti];
			}
		}
	}
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
		char *s = value_to_column(&info->value, true, error);
		if (error->error) {
			return;
		}
		csvtmt_csvline_set_clone(row, info->index, s, error);
		free(s);
	}
}

static ColumnInfo *
where_match(ColumnInfoArray *where_infos, CsvTomatoCsvLine *row) {
	for (size_t ii = 0; ii < where_infos->len; ii++) {
		ColumnInfo *winfo = &where_infos->array[ii];

		for (size_t ci = 0; ci < row->len; ci++) {
			const char *col = row->columns[ci];
			if (ci == winfo->index) {
				CsvTomatoValue val = string_to_value(col);
				if (value_eq(&winfo->value, &val)) {
					return winfo;
				}
			}
		}
	}	
	return NULL;
}

CsvTomatoResult
csvtmt_delete(CsvTomatoModel *model, CsvTomatoError *error) {
	bool has_where = model->where_key_values_len;
	CsvTomatoCsvLine row = {0};
	
	header_read_from_table(&model->header, model->table_path, error);
	if (error->error) {
		goto failed_to_header_read;
	}

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

	if (has_where) {
		// DELETEではWHEREにマッチする行は論理削除を行う。
		ColumnInfoArray where_infos = {0};

		store_colinfo(model, &where_infos, model->where_key_values, model->where_key_values_len, error);
		if (error->error) {
			goto array_overflow;
		}

		CsvTomatoCsvLine row = {0};
		char *p = ptr;

		// skip first line (header)
		p = (char *) csvtmt_csvline_parse_string(&row, p, error);
		if (error->error) {
			goto failed_to_parse_csv;
		}
		csvtmt_csvline_final(&row);

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
				csvtmt_csvline_final(&row);
				continue; // this line deleted
			}

			ColumnInfo *winfo = where_match(&where_infos, &row);
			if (winfo) {
				// match
				if (*head == '"') {
					head++;
				}
				// __MODE__ column to 1 (delete)
				*head = '1';
			}

			csvtmt_csvline_final(&row);
		}


	} else {
		// 全論理削除。
		char *p = ptr;

		// skip first line (header)
		p = (char *) csvtmt_csvline_parse_string(&row, p, error);
		if (error->error) {
			goto failed_to_parse_csv;
		}
		csvtmt_csvline_final(&row);

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
				csvtmt_csvline_final(&row);
				continue; // this line deleted
			}
			if (*head == '"') {
				head++;
			}
			// __MODE__ column to 1 (delete)
			*head = '1';

			csvtmt_csvline_final(&row);
		}
	}

	{ // Linux
		munmap(ptr, size);
		close(fd);
	}

	return CSVTMT_DONE;
failed_to_header_read:
	return CSVTMT_ERROR;
failed_to_parse_csv:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse csv line");
	return CSVTMT_ERROR;
array_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "array overflow");
	return CSVTMT_ERROR;
failed_to_stat:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to get table size: %s", strerror(errno));
	return CSVTMT_ERROR;
failed_to_mmap:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to mmap. %s", strerror(errno));
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return CSVTMT_ERROR;
}

CsvTomatoResult
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
		csvtmt_csvline_final(&row);

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
				csvtmt_csvline_final(&row);
				continue; // this line deleted
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
							*head = '1'; // __MODE__ column to 1 (delete)
							// printf("head[%s]\n", head);
						}
					}
				}
			}

			csvtmt_csvline_final(&row);
		}

		append_csv_lines(model, &lines, false, error);
		if (error->error) {
			goto failed_to_append_lines;
		}

		for (size_t i = 0; i < lines.len; i++) {
			csvtmt_csvline_final(&lines.array[i]);
		}

		{ // Linux
			munmap(ptr, size);
			close(fd);
		}
	} else {
		// UPDATE table SET age = 123, name = "Taro"
		// WHERE句がない。全置換。
		update_all(model, error);
		if (error->error) {
			goto failed_to_replace_update;
		}
	}

	return CSVTMT_OK;

failed_to_header_read:
	return CSVTMT_ERROR;
failed_to_replace_update:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to replace update");
	return CSVTMT_ERROR;
invalid_key_values_type:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid update key. \"%s\" is not in header types", not_found);
	return CSVTMT_ERROR;
failed_to_stat:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to get table size: %s", strerror(errno));
	return CSVTMT_ERROR;
failed_to_mmap:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to mmap. %s", strerror(errno));
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return CSVTMT_ERROR;
failed_to_parse_csv:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse csv line");
	return CSVTMT_ERROR;
array_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "array overflow");
	return CSVTMT_ERROR;
failed_to_append_lines:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to append csv lines");
	return CSVTMT_ERROR;
}

CsvTomatoResult
csvtmt_select(CsvTomatoModel *model, CsvTomatoError *error) {
	// SELECTではmmapを使って走査する。
	const char *not_found = NULL;
	CsvTomatoCsvLine row = {0};
	bool has_where = model->where_key_values_len;

	if (model->mmap.fd == 0) {
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

		{ // Linux
			errno = 0;
			model->mmap.fd = open(model->table_path, O_RDWR);
			if (model->mmap.fd == -1) {
				goto failed_to_open_table;
			}

			struct stat st;
			errno = 0;
			if (fstat(model->mmap.fd, &st) == -1) {
				close(model->mmap.fd);
				model->mmap.fd = 0;
				goto failed_to_stat;
			}

			model->mmap.size = st.st_size;

			model->mmap.ptr = mmap(
				NULL, 
				model->mmap.size, 
				PROT_READ, 
				MAP_SHARED, 
				model->mmap.fd, 
				0
			);
			if (model->mmap.ptr == MAP_FAILED) {
				goto failed_to_mmap;
			}

			model->mmap.cur = model->mmap.ptr;
		}

		// skip first line (header)
		model->mmap.cur = (char *) csvtmt_csvline_parse_string(&row, model->mmap.cur, error);
		if (error->error) {
			goto failed_to_parse_csv;
		}
		csvtmt_csvline_final(&row);
	}

	// fdが0じゃない場合は（すでにファイルを開いている場合は）
	// 途中から続行する。

	if (has_where) {
		ColumnInfoArray where_infos = {0};
		store_colinfo(model, &where_infos, model->where_key_values, model->where_key_values_len, error);
		if (error->error) {
			goto failed_to_store_colinfo;
		}

		for (; *model->mmap.cur; ) {
			model->mmap.cur = (char *) csvtmt_csvline_parse_string(&row, model->mmap.cur, error);
			if (error->error) {
				goto failed_to_parse_csv;
			}
			if (!strcmp(row.columns[0], "1")) {
				csvtmt_csvline_final(&row);
				continue; // this line deleted
			}

			ColumnInfo *winfo = where_match(&where_infos, &row);
			if (winfo) {
				model->row = row;
				store_selected_columns(model, &model->row, error);
				if (error->error) {
					goto failed_to_store_selected_columns;
				}
				memset(&row, 0, sizeof(row));
				goto ret_row;
			}

			csvtmt_csvline_final(&row);
		}
	} else {
		for (; *model->mmap.cur; ) {
			model->mmap.cur = (char *) csvtmt_csvline_parse_string(&row, model->mmap.cur, error);
			if (error->error) {
				goto failed_to_parse_csv;
			}
			if (!strcmp(row.columns[0], "1")) {
				csvtmt_csvline_final(&row);
				continue; // this line deleted
			}

			model->row = row;

			store_selected_columns(model, &model->row, error);
			if (error->error) {
				goto failed_to_store_selected_columns;
			}

			goto ret_row;
		}
	}

	// ここに到達したらファイルを閉じる。
	{
		munmap(model->mmap.ptr, model->mmap.size);
		close(model->mmap.fd);
		model->mmap.ptr = NULL;
		model->mmap.fd = 0;
	}

	return CSVTMT_DONE;	
ret_row:
	// ここに到達したらファイルを開いたまま処理を継続する。
	return CSVTMT_ROW;
failed_to_store_selected_columns:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to store selected columns");
	return CSVTMT_ERROR;
failed_to_store_colinfo:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to store column info");
	return CSVTMT_ERROR;
failed_to_header_read:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to read header");
	return CSVTMT_ERROR;
failed_to_parse_csv:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to parse csv line");
	return CSVTMT_ERROR;
failed_to_mmap:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to mmap. %s", strerror(errno));
	return CSVTMT_ERROR;
failed_to_stat:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to get table size: %s", strerror(errno));
	return CSVTMT_ERROR;
invalid_column:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid column name. \"%s\" is not in header types", not_found);
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return CSVTMT_ERROR;
}

CsvTomatoResult
csvtmt_insert(CsvTomatoModel *model, CsvTomatoError *error) {
	// INSERTでは単純なファイルへの追記を行う。
	const char *not_found = NULL;
	FILE *fp = NULL;
	CsvTomatoString *buf = NULL;
	
	#undef cleanup
	#define cleanup() {\
		if (fp) {\
			fclose(fp);\
		}\
		csvtmt_str_del(buf);\
	}\

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
	fp = fopen(model->table_path, "a");
	if (!fp) {
		goto failed_to_open_table;
	}

	buf = csvtmt_str_new();
	if (!buf) {
		goto failed_to_allocate_buffer;
	}

	// valuesは二次元配列。
	// VALUES (1, 2), (3, 4), (5, 6)
	// などに対応する。
	for (size_t i = 0; i < model->values_len; i++) {
		CsvTomatoValues *values = &model->values[i];

		if (values->len != model->column_names_len) {
			printf("%ld %ld %ld\n", model->values_len, values->len, model->column_names_len);
			goto invalid_values_len;
		}

		csvtmt_str_clear(buf);

		// types:t1,t2,t3
		// column_names:t1,t3
		// values:1,3
		for (size_t j = 0; j < model->header.types_len; j++) {
			const CsvTomatoColumnType *type = &model->header.types[j];
			if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
				csvtmt_str_append(buf, "0,");
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
				csvtmt_str_append(buf, col);
			} else {
				// values_indexはカラムに含まれている。
				if (values_index >= values->len) {
					goto values_index_out_of_ragen;
				}
				const CsvTomatoValue *value = &values->values[values_index];
				char sbuf[1024];
				// printf("value %d\n", value->kind);

				switch (value->kind) {
				default: goto invalid_value_kind; break;
				case CSVTMT_VAL_INT:
					snprintf(sbuf, sizeof sbuf, "%ld", value->int_value);
					csvtmt_str_append(buf, sbuf);
					break;
				case CSVTMT_VAL_DOUBLE:
					snprintf(sbuf, sizeof sbuf, "%f", value->double_value);
					csvtmt_str_append(buf, sbuf);
					break;
				case CSVTMT_VAL_STRING: {
					// TODO: wrap by double-quote
					assert(value->string_value);
					char *s = csvtmt_wrap_column(value->string_value, error);
					if (!s || error->error) {
						goto failed_to_wrap_column;
					}
					csvtmt_str_append(buf, s);
					free(s);
				} break;
				}
			}

			csvtmt_str_push_back(buf, ',');
		}

		csvtmt_str_pop_back(buf); // ,
		fprintf(fp, "%s\n", buf->str);
	}

	cleanup();
	return CSVTMT_OK;

failed_to_wrap_column:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to wrap column");
	cleanup();
	return CSVTMT_ERROR;
failed_to_allocate_buffer:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to allocate buffer");
	cleanup();
	return CSVTMT_ERROR;
failed_to_gen_type_string:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to generate type column value");
	cleanup();
	return CSVTMT_ERROR;
failed_to_header_read:
	cleanup();
	return CSVTMT_ERROR;
invalid_column:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid column name. \"%s\" is not in header types", not_found);
	cleanup();
	return CSVTMT_ERROR;
invalid_values_len:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid values length. column names len is \"%ld\" but values length is \"%ld\"", model->column_names_len, model->values_len);
	cleanup();
	return CSVTMT_ERROR;
invalid_value_kind:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid value kind");
	cleanup();
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	cleanup();
	return CSVTMT_ERROR;
values_index_out_of_ragen:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "values index out of range");
	cleanup();
	return CSVTMT_ERROR;
}