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

	#define stack_push(o) {\
		if (model->stack_len >= csvtmt_numof(model->stack)) {\
			goto stack_overflow;\
		}\
		model->stack[model->stack_len++] = o;\
	}\

	#define stack_push_kind(k) {\
		if (model->stack_len >= csvtmt_numof(model->stack)) {\
			goto stack_overflow;\
		}\
		CsvTomatoStackElem o = {.kind=k};\
		model->stack[model->stack_len++] = o;\
	}\

	#define stack_pop(dst) {\
		if (model->stack_len == 0) {\
			goto stack_underflow;\
		}\
		dst = model->stack[--model->stack_len];\
	}\

	memset(model, 0, sizeof(*model));

	snprintf(model->db_dir, sizeof model->db_dir, "%s", db_dir);
	model->buf = csvtmt_str_new();
	if (!model->buf) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate buffer: %s", strerror(errno));
		return;
	}

	CsvTomatoOpcodeKind op_kind;
	CsvTomatoStackElem pop;
	model->stack_len = 0;

	for (size_t i = 0; i < opcodes_len; i++) {
		const CsvTomatoOpcodeElem *op = &opcodes[i];

		switch (op->kind) {
		case CSVTMT_OP_NONE: 
			op_kind = op->kind;
			goto invalid_op_kind;
			break;
		case CSVTMT_OP_IDENT: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_IDENT;
			elem.obj.ident.value = op->obj.ident.value;
			stack_push(elem);
		} break;
		case CSVTMT_OP_UPDATE_STMT_BEG: {
			model->table_name = op->obj.update_stmt.table_name;
			snprintf(model->table_path, sizeof model->table_path, "%s/%s.csv", model->db_dir, model->table_name);
			model->update_set_key_values_len = 0;
			model->where_key_values_len = 0;
		} break;
		case CSVTMT_OP_UPDATE_STMT_END: {
			csvtmt_update(model, error);
			if (error->error) {
				goto update_error;
			}
		} break;
		case CSVTMT_OP_INSERT_STMT_BEG: {
			model->table_name = op->obj.insert_stmt.table_name;
			model->column_names_len = 0;
			model->values_len = 0;

			snprintf(model->table_path, sizeof model->table_path, "%s/%s.csv", model->db_dir, model->table_name);
		} break;
		case CSVTMT_OP_INSERT_STMT_END: {
			csvtmt_insert(model, error);
			if (error->error) {
				goto insert_error;
			}			
		} break;
		case CSVTMT_OP_COLUMN_NAMES_BEG: {
			stack_push_kind(CSVTMT_STACK_ELEM_COLUMN_NAMES_BEG);
		} break;
		case CSVTMT_OP_COLUMN_NAMES_END: {
			while (model->stack_len) {
				stack_pop(pop);
				if (pop.kind == CSVTMT_STACK_ELEM_COLUMN_NAMES_BEG) {
					break;
				}
				switch (pop.kind) {
				default: goto invalid_op_kind; break;
				case CSVTMT_STACK_ELEM_STRING_VALUE:
					check_array(model->column_names);
					model->column_names[model->column_names_len++] = pop.obj.string_value.value;
					break;
				}
			}
			break;
		} break;
		case CSVTMT_OP_VALUES_BEG: {
			if (model->values_len >= csvtmt_numof(model->values)) {
				goto array_overflow;
			}
			stack_push_kind(CSVTMT_STACK_ELEM_VALUES_BEG);
		} break;
		case CSVTMT_OP_VALUES_END: {
			while (model->stack_len) {
				stack_pop(pop);
				if (pop.kind == CSVTMT_STACK_ELEM_VALUES_BEG) {
					break;
				}
				if (model->values_len >= csvtmt_numof(model->values)) {
					goto array_overflow;
				}
				CsvTomatoValues *values = &model->values[model->values_len];
				if (values->len >= csvtmt_numof(values->values)) {
					goto array_overflow;
				}
				CsvTomatoValue *value = &values->values[values->len];

				switch (pop.kind) {
				default: goto invalid_op_kind; break;
				case CSVTMT_STACK_ELEM_INT_VALUE:
					value->kind = CSVTMT_VAL_INT;
					value->int_value = pop.obj.int_value.value;
					break;
				case CSVTMT_STACK_ELEM_FLOAT_VALUE:
					value->kind = CSVTMT_VAL_FLOAT;
					value->float_value = pop.obj.float_value.value;
					break;
				case CSVTMT_STACK_ELEM_STRING_VALUE:
					value->kind = CSVTMT_VAL_STRING;
					value->string_value = pop.obj.string_value.value;
					break;
				}

				values->len++;				
				// printf("values->len %ld\n", values->len);
			}

			model->values_len++;
			// printf("values_len %ld\n", model->values_len);
		} break;
		case CSVTMT_OP_UPDATE_SET_BEG: {
			model->update_set_key_values_len = 0;
			stack_push_kind(CSVTMT_STACK_ELEM_UPDATE_SET_BEG);
		} break;
		case CSVTMT_OP_UPDATE_SET_END: {
			while (model->stack_len) {
				stack_pop(pop);

				if (pop.kind == CSVTMT_STACK_ELEM_UPDATE_SET_BEG) {
					break;
				}
				switch (pop.kind) {
				default: goto invalid_stack_elem_kind; break;
				case CSVTMT_STACK_ELEM_KEY_VALUE: {
					if (model->update_set_key_values_len >= csvtmt_numof(model->update_set_key_values)) {
						goto array_overflow;
					}
				 	CsvTomatoKeyValue *kv =  &model->update_set_key_values[model->update_set_key_values_len++];
				 	kv->key = pop.obj.key_value.key;
				 	kv->value = pop.obj.key_value.value;
				 	// printf("kv->key[%s]\n", kv->key);
				} break;
				}
			}
		} break;
		case CSVTMT_OP_UPDATE_WHERE_BEG: {
			model->where_key_values_len = 0;
			stack_push_kind(CSVTMT_STACK_ELEM_UPDATE_WHERE_BEG);
		} break;
		case CSVTMT_OP_UPDATE_WHERE_END: {
			while (model->stack_len) {
				stack_pop(pop);

				if (pop.kind == CSVTMT_STACK_ELEM_UPDATE_WHERE_BEG) {
					break;
				}
				switch (pop.kind) {
				default: goto invalid_stack_elem_kind; break;
				case CSVTMT_STACK_ELEM_KEY_VALUE: {
					if (model->where_key_values_len >= csvtmt_numof(model->update_set_key_values)) {
						goto array_overflow;
					}
				 	CsvTomatoKeyValue *kv =  &model->where_key_values[model->where_key_values_len++];
				 	kv->key = pop.obj.key_value.key;
				 	kv->value = pop.obj.key_value.value;
				 	// printf("where kv->key[%s]\n", kv->key);
				} break;
				}
			}
		} break;
		case CSVTMT_OP_ASSIGN: {
			CsvTomatoStackElem lhs, rhs;
			stack_pop(rhs);
			stack_pop(lhs);

			switch (lhs.kind) {
			default: goto invalid_op_kind; break;
			case CSVTMT_STACK_ELEM_IDENT: {
				switch (rhs.kind) {
				default: goto invalid_op_kind; break;
				case CSVTMT_STACK_ELEM_INT_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE,
					elem.obj.key_value.key = lhs.obj.ident.value;
					elem.obj.key_value.value.kind = CSVTMT_VAL_INT;
					elem.obj.key_value.value.int_value = rhs.obj.int_value.value;
					stack_push(elem);
				} break;
				case CSVTMT_STACK_ELEM_FLOAT_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE,
					elem.obj.key_value.key = lhs.obj.ident.value;
					elem.obj.key_value.value.kind = CSVTMT_VAL_FLOAT;
					elem.obj.key_value.value.float_value = rhs.obj.float_value.value;
					stack_push(elem);
				} break;
				case CSVTMT_STACK_ELEM_STRING_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE,
					elem.obj.key_value.key = lhs.obj.ident.value;
					elem.obj.key_value.value.kind = CSVTMT_VAL_STRING;
					elem.obj.key_value.value.string_value = rhs.obj.string_value.value;
					stack_push(elem);
				} break;
				}
			} break;
			}
		} break;
		case CSVTMT_OP_STRING_VALUE: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_STRING_VALUE;
			elem.obj.string_value.value = op->obj.string_value.value;
			stack_push(elem);
		} break;
		case CSVTMT_OP_INT_VALUE: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_INT_VALUE;
			elem.obj.int_value.value = op->obj.int_value.value;
			stack_push(elem);
		} break;
		case CSVTMT_OP_FLOAT_VALUE: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_FLOAT_VALUE;
			elem.obj.float_value.value = op->obj.float_value.value;
			stack_push(elem);
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

			// buf stored column def strings
			csvtmt_str_pop_back(model->buf); // last ,

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
stack_overflow:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "stack overflow");
	cleanup();
	return;
stack_underflow:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "stack underflow");
	cleanup();
	return;
invalid_stack_elem_kind:
	csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid stack element kind");
	cleanup();
	return;
update_error:
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
