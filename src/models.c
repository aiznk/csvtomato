#include <csvtomato.h>

// id INTEGER PRIMARY KEY AUTOINCREMENT
// name TEXT NOT NULL
// とかがCSVヘッダカラム。idやnameをtype_nameに、それ以降をtype_defに格納。
void
parse_type_column(CsvTomatoColumnType *type, const char *col, CsvTomatoError *error) {
	char *name = type->type_name;
	size_t name_len = 0;
	size_t name_size = sizeof(type->type_name);
	char *def = type->type_def;
	size_t def_len = 0;
	size_t def_size = sizeof(type->type_def);
	int m = 0;

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

	return;
overflow:
	csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "buffer overflow in parse type column");
}

static void
header_read(CsvTomatoHeader *self, const char *table_path, CsvTomatoError *error) {
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
			csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "header types overflow");
			return;
		}
		CsvTomatoColumnType *type = &self->types[self->types_len++];
		type->index = i;
		parse_type_column(type, row.columns[i], error);
		if (error->error) {
			return;	
		}
		// printf("type: index[%ld] type_name[%s] type_def[%s]\n", type->index, type->type_name, type->type_def);
	}

	csvtmt_csvline_destroy(&row);
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

	puts(id_path);
	FILE *id_fp = fopen(id_path, "r+");
	if (!id_fp) {
		goto failed_to_open_file;
	}

	uint64_t id;
	char line[1024];

	fgets(line, sizeof line, id_fp);
	id = atoi(line);

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
	const char *s = type->type_def;
	printf("type_name[%s] type_def[%s]\n", type->type_name, s);

	if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
		strcpy(dst, "0");
	} else if (strstr(s, "INTEGER")) {
		if (strstr(s, "AUTOINCREMENT")) {
			uint64_t id = gen_auto_increment_id(model, type->type_name, error);
			if (error->error) {
				goto fail_gen_id;
			}
			snprintf(dst, dst_size, "%ld", id);
		} else {
			strcpy(dst, "0");
		}
	} else if (strstr(s, "TEXT")) {
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

void
csvtmt_insert(CsvTomatoModel *model, CsvTomatoError *error) {
	// INSERTでは単純なファイルへの追記を行う。
	const char *not_found = NULL;
	
	header_read(&model->header, model->table_path, error);
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

	if (model->values_len != model->column_names_len) {
		goto invalid_values_len;
	}

	csvtmt_str_clear(model->buf);

	// types:t1,t2,t3
	// column_names:t1,t3
	// values:1,3
	for (size_t i = 0; i < model->header.types_len; i++) {
		const CsvTomatoColumnType *type = &model->header.types[i];
		if (!strcmp(type->type_name, CSVTMT_COL_MODE)) {
			csvtmt_str_append(model->buf, "0,");
			continue;
		} 

		// INSERT INTO table (id, name) VALUES (0, "Alice")
		// INSERT INTO table (id, name) VALUES (0, "Alice")

		int values_index = -1;
		for (size_t j = 0; j < model->column_names_len; j++) {
			const char *column_name = model->column_names[j];
			if (!strcmp(type->type_name, column_name)) {
				values_index = j;
				break;
			}
		}
		if (values_index == -1) {
			// i は (id, name) などで指定されていないカラム。
			// typesの情報を元にデフォルト値を入れる。
			char col[1024];
			type_gen_column_default_value(model, type, col, sizeof col, error);
			if (error->error) {
				goto failed_to_gen_type_string;
			}
			csvtmt_str_append(model->buf, col);
		} else {
			// values_indexはカラムに含まれている。
			const CsvTomatoValue *value = &model->values[values_index];
			char buf[1024];

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

	// write to file
	errno = 0;
	FILE *fp = fopen(model->table_path, "a");
	if (!fp) {
		goto failed_to_open_table;
	}

	fprintf(fp, "%s\n", model->buf->str);

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
invalid_value_kind:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid value kind");
	return;
failed_to_open_table:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "failed to open table %s. %s", model->table_path, strerror(errno));
	return;
}