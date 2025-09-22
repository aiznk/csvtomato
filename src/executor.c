#include <csvtomato.h>

CsvTomatoExecutor *
csvtmt_executor_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoExecutor *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	return self;
}

void
csvtmt_executor_del(CsvTomatoExecutor *self) {
	if (!self) {
		return;
	}

	free(self);
}

void
csvtmt_executor_exec(
	CsvTomatoExecutor *self,
	CsvTomatoModel *model,
	const char *db_dir,
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoError *error
) {
	#define check_array(IDENT) {\
		if (IDENT ## _len >= csvtmt_numof(IDENT)) {\
			goto array_overflow;\
		}\
	}\

	#define cleanup() {\
		csvtmt_str_del(model->buf);\
		model->buf = NULL;\
	}\

	snprintf(model->db_dir, sizeof model->db_dir, "%s", db_dir);
	model->buf = csvtmt_str_new();
	if (!model->buf) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate buffer: %s", strerror(errno));
		return;
	}

	CsvTomatoOpcodeKind op_kind;

	for (size_t i = 0; i < opcodes_len; i++) {
		const CsvTomatoOpcodeElem *op = &opcodes[i];

		switch (op->kind) {
		case CSVTMT_OP_NONE: 
			op_kind = op->kind;
			goto invalid_op_kind;
			break;
		case CSVTMT_OP_INSERT_STMT_BEG: {
			model->table_name = op->obj.insert_stmt.table_name;
			snprintf(model->table_path, sizeof model->table_path, "%s/%s.csv", model->db_dir, model->table_name);
		} break;
		case CSVTMT_OP_INSERT_STMT_END: {
			csvtmt_insert(model, error);
			if (error->error) {
				goto insert_error;
			}			
		} break;
		case CSVTMT_OP_COLUMN_NAMES_BEG: {
			model->column_names_len = 0;
			model->mode = CSVTMT_MODE_COLUMN_NAMES;
		} break;
		case CSVTMT_OP_COLUMN_NAMES_END: {
			model->mode = CSVTMT_MODE_NONE;
		} break;
		case CSVTMT_OP_VALUES_BEG: {
			model->values_len = 0;
			model->mode = CSVTMT_MODE_VALUES;
		} break;
		case CSVTMT_OP_VALUES_END: {
			model->mode = CSVTMT_MODE_NONE;
		} break;
		case CSVTMT_OP_STRING_VALUE: {
			switch (model->mode) {
			case CSVTMT_MODE_NONE: break;
			case CSVTMT_MODE_COLUMN_NAMES:
				check_array(model->column_names);
				model->column_names[model->column_names_len++] = op->obj.string_value.value;
				break;
			case CSVTMT_MODE_VALUES:
				check_array(model->values);
				model->values[model->values_len].kind = CSVTMT_VAL_STRING;
				model->values[model->values_len].string_value = op->obj.string_value.value;
				model->values_len++;
				break;
			}
		} break;
		case CSVTMT_OP_INT_VALUE: {
			switch (model->mode) {
			default: break;
			case CSVTMT_MODE_VALUES:
				check_array(model->values);
				model->values[model->values_len].kind = CSVTMT_VAL_INT;
				model->values[model->values_len].int_value = op->obj.int_value.value;
				model->values_len++;				
				break;
			}
		} break;
		case CSVTMT_OP_FLOAT_VALUE: {
			switch (model->mode) {
			default: break;
			case CSVTMT_MODE_VALUES:
				check_array(model->values);
				model->values[model->values_len].kind = CSVTMT_VAL_FLOAT;
				model->values[model->values_len].float_value = op->obj.float_value.value;
				model->values_len++;				
				break;
			}
		} break;
		case CSVTMT_OP_CREATE_TABLE_STMT_BEG: {
			model->table_name = op->obj.create_table_stmt.table_name;
			assert(model->table_name);
			bool if_not_exists = op->obj.create_table_stmt.if_not_exists;

			snprintf(model->table_path, sizeof model->table_path, "%s/%s.csv", model->db_dir, model->table_name);

			if (csvtmt_file_exists(model->table_path)) {
				model->do_create_table = false;
				if (!if_not_exists) {
					csvtmt_error_format(error, CSVTMT_ERR_EXEC, "table %s already exists", model->table_name);
					return;
				}
			} else {
				model->do_create_table = true;
				// 論理削除カラム（予約カラム）をヘッダにセット。
				csvtmt_str_append(model->buf, CSVTMT_COL_MODE);
				csvtmt_str_append(model->buf, ",");
			}
		} break;
		case CSVTMT_OP_CREATE_TABLE_STMT_END: {
			if (!model->do_create_table) {
				continue;
			}

			csvtmt_str_pop_back(model->buf);

			errno = 0;
			FILE *fp = fopen(model->table_path, "w");
			if (!fp) {
				csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table %s: %s", model->table_path, strerror(errno));
				return;
			}

			fprintf(fp, "%s\n", model->buf->str);

			fclose(fp);
			csvtmt_str_clear(model->buf);

			model->do_create_table = false;
		} break;
		case CSVTMT_OP_COLUMN_DEF: {
			if (!model->do_create_table) {
				continue;
			}

			const char *column_name = op->obj.column_def.column_name;
			csvtmt_str_append(model->buf, column_name);

			const CsvTomatoTokenKind type_name = op->obj.column_def.type_name;
			switch (type_name) {
			default: 
				csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid type name: %d", type_name);
				return;
				break;
			case CSVTMT_TK_INTEGER: csvtmt_str_append(model->buf, " INTEGER"); break;
			case CSVTMT_TK_TEXT: csvtmt_str_append(model->buf, " TEXT"); break;
			}

			if (op->obj.column_def.primary &&
				op->obj.column_def.key) {
				csvtmt_str_append(model->buf, " PRIMARY KEY");
			}
			if (op->obj.column_def.autoincrement) {
				csvtmt_str_append(model->buf, " AUTOINCREMENT");
			}
			if (op->obj.column_def.not_ &&
				op->obj.column_def.null) {
				csvtmt_str_append(model->buf, " NOT NULL");
			}

			csvtmt_str_push_back(model->buf, ',');
		} break;
		}
	}

	cleanup();
	return;
insert_error:
	cleanup();
	return;	
array_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "array overflow");
	cleanup();
	return;
invalid_op_kind:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid op kind %d", op_kind);
	cleanup();
	return;
}
