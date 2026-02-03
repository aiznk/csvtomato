#include <csvtomato.h>

CsvTomatoExecutor *
csvtmt_executor_new(CsvTomatoError *error) {
	errno = 0;
	CsvTomatoExecutor *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
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

static void
store_table_path(CsvTomatoModel *model, const char *table_name) {
	snprintf(model->table_path, sizeof model->table_path, "%s/%s.csv", model->db_dir, table_name);
}

size_t 
skip_to(
	CsvTomatoModel *self,
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoOpcodeKind kind,
	CsvTomatoError *error
) {
	for (size_t i = self->opcodes_index; i < opcodes_len; i++) {
		const CsvTomatoOpcodeElem *op = &opcodes[i];
		if (op->kind == kind) {
			return i;
		}
	}
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "not found opcode kind on skip");
	return 0;
}

typedef struct {
	CsvTomatoFuncKind kind;
	const char *column_name;
	bool star;
} ExecFunc;

CsvTomatoResult
csvtmt_executor_exec(
	CsvTomatoExecutor *self,
	CsvTomatoModel *model,
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
		csvtmt_str_del(buf);\
		buf = NULL;\
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

	#define stack_top(dst) {\
		if (model->stack_len == 0) {\
			goto stack_underflow;\
		}\
		dst = model->stack[model->stack_len-1];\
	}\

	#define restore_save_index() {\
		if (model->save_opcodes_index == 0) {\
			model->opcodes_index = 0;\
		} else {\
			model->opcodes_index = model->save_opcodes_index-1;\
		}\
	}\

	// puts("exec");
	CsvTomatoString *buf = csvtmt_str_new();
	if (!buf) {
		goto failed_to_allocate_buffer;
	}

	CsvTomatoStackElem pop;
	CsvTomatoResult result = CSVTMT_DONE;
	const char *not_found;
	CsvTomatoOpcodeKind cur_context;
	ExecFunc select_funcs[100];
	size_t select_funcs_i = 0;

	model->stack_len = 0;

	for (; model->opcodes_index < opcodes_len; ) {
		const CsvTomatoOpcodeElem *op = &opcodes[model->opcodes_index];

		switch (op->kind) {
		case CSVTMT_OP_NONE: 
			goto invalid_op_kind;
			break;
		case CSVTMT_OP_FUNC_BEG:
			break;
		case CSVTMT_OP_FUNC_END: {
			CsvTomatoStackElem top;
			stack_pop(top);

			ExecFunc *func = NULL;
			switch (cur_context) {
			default: goto invalid_context; break;
			case CSVTMT_OP_SELECT_STMT_BEG:
				// TODO: fix index out of range
				func = &select_funcs[select_funcs_i++];
				break;
			}

			if (func) {
				switch (top.kind) {
				case CSVTMT_STACK_ELEM_STAR:
					func->star = true;
					break;
				case CSVTMT_STACK_ELEM_STRING_VALUE:
					func->column_name = top.obj.string_value.value;
					break;
				}
			}
		} break;
		case CSVTMT_OP_STAR: {
			stack_push_kind(CSVTMT_STACK_ELEM_STAR);
		} break;
		case CSVTMT_OP_PLACE_HOLDER: {
			goto found_place_holder;
		} break;
		case CSVTMT_OP_SHOW_TABLES_BEG: {
			model->db_name = op->obj.show_tables_stmt.db_name;
			csvtmt_show_tables(model, error);
		} break;
		case CSVTMT_OP_SHOW_TABLES_END: {
			goto done;
		} break;
		/*
			UPDATE users SET age = 1, name = "Taro" WHERE age == 1 AND name = "Ken"; 
			↓
			[
				update_beg,
					update_set_beg,
						ident, int, assign,
						ident, string, assign,
					update_set_end,
					where_beg,
						ident, int, equal,
						ident, string, equal,
						and,
					where_end,
				update_end,
			] {
				update_beg {
					if !opened {
						open_mmap
					}
					row = read_row
				}
				update_set_beg {
					push update_set_beg -> [update_set_beg]
				}
				push ident -> [update_set_beg, ident]
				push int -> [update_set_beg, ident, int]
				assign {
					int = pop <- [update_set_beg, ident, int]
					ident = pop <- [update_set_beg, ident]
					push key_value(ident, int) -> [update_set_beg, key_value]
				}
				push ident -> [update_set_beg, key_value, ident]
				push string -> [update_set_beg, key_value, ident, string]
				assign {
					string = pop <- [update_set_beg, key_value, ident, string]
					ident = pop <- [update_set_beg, key_value, ident]
					push key_value(ident, string) -> [update_set_beg, key_value, key_value]
				}
				where_beg {
				}
				push ident -> [update_set_beg, key_value, key_value, ident]
				push int -> [update_set_beg, key_value, key_value, ident, int]
				equal {
					pop int <- [update_set_beg, key_value, key_value, ident, int]
					pop ident <- [update_set_beg, key_value, key_value, ident]
					push (row[ident] == int) -> [update_set_beg, key_value, key_value, bool]
				}
				push ident -> [update_set_beg, key_value, key_value, bool, ident]
				push string -> [update_set_beg, key_value, key_value, bool, ident, string]
				equal {
					pop string <- [update_set_beg, key_value, key_value, bool, ident, string]
					pop ident <- [update_set_beg, key_value, key_value, bool, ident]
					push (row[ident] == string) -> [update_set_beg, key_value, key_value, bool, bool]
				}
				and {
					lhs = pop <- [update_set_beg, key_value, key_value, bool, bool]
					rhs = pop <- [update_set_beg, key_value, key_value, bool]
					push (lhs == rhs) -> [update_set_beg, key_value, key_value, bool]
				}
				update_end {
					if stack_top is not bool {
						// update all
						replace_row(row)
						push_rows(row)
					} else {
						bool = pop <- [update_set_beg, key_value, key_value, bool]
						if bool {
							replace_row(row)
							push_rows(row)
						}
					}
					if *ptr == '\0' {
						close_mmap
						open_fp
						append_rows
					} else {
						goto update_beg;
					}
				}
			}
		*/
		case CSVTMT_OP_UPDATE_STMT_BEG: {
			// puts("update beg");
			if (model->mmap.fd == 0) {
				model->table_name = op->obj.update_stmt.table_name;
				store_table_path(model, model->table_name);
				model->update_set_key_values_len = 0;

				csvtmt_open_mmap_for_read_write(model, model->table_path, error);
				if (error->error) {
					goto failed_to_open_mmap;
				}				

				model->mmap.cur = (char *) csvtmt_header_read_from_string(&model->header, model->mmap.cur, error);
				if (error->error) {
					goto failed_to_read_header;
				}

				if (model->rows) {
					csvtmt_clear_rows(model->rows);
				} else {
					model->rows = csvtmt_rows_new();
					if (!model->rows) {
						goto failed_to_allocate_rows;
					}
				}
			}

			model->save_opcodes_index = model->opcodes_index;
			model->row_head = model->mmap.cur;
			model->skip = false;

			csvtmt_parse_row_from_mmap(model, error);
			if (error->error) {
				goto failed_to_parse_row;
			}
			if (csvtmt_is_deleted_row(&model->row)) {
				model->opcodes_index = skip_to(
					model,
					opcodes,
					opcodes_len,
					CSVTMT_OP_UPDATE_STMT_END,
					error
				);
				model->skip = true;
				csvtmt_row_final(&model->row);
				continue;
			}
		} break;
		case CSVTMT_OP_UPDATE_STMT_END: {
			// puts("update end");
			if (model->skip) {
				if (*model->mmap.cur == '\0') {
					csvtmt_close_mmap(model);
					csvtmt_append_rows_to_table(
						model->table_path,
						model->rows,
						false,
						error
					);
					if (error->error) {
						goto failed_to_append_rows;
					}
					csvtmt_clear_rows(model->rows);
					csvtmt_row_final(&model->row);
					model->opcodes_index++;
				} else {
					restore_save_index();
				}
				continue;
			}

			not_found = csvtmt_header_has_key_values_types(
				&model->header,
				model->update_set_key_values,
				model->update_set_key_values_len,
				error
			);
			if (not_found) {
				goto not_found_keys;
			}

			if (model->stack_len) {
				CsvTomatoStackElem top;
				stack_top(top);

				if (top.kind != CSVTMT_STACK_ELEM_BOOL_VALUE) {
					csvtmt_close_mmap(model);
					csvtmt_update_all(model, error);
					if (error->error) {
						goto failed_to_update_all;
					}
					csvtmt_clear_rows(model->rows);
					csvtmt_row_final(&model->row);
				} else {
					if (top.obj.bool_value.value) {
						// match WHERE
						CsvTomatoColumnInfoArray infos = {0};
						csvtmt_store_column_infos(
							model,
							&infos,
							model->update_set_key_values,
							model->update_set_key_values_len,
							error
						);
						if (error->error) {
							goto failed_to_store_col_infos;
						}
						csvtmt_delete_row_head(model);
						csvtmt_replace_row(model, &model->row, &infos, error);
						if (error->error) {
							goto failed_to_replace_row;
						}
						if (!csvtmt_rows_push_back(model->rows, model->row)) {
							goto failed_to_push_row;
						}
						memset(&model->row, 0, sizeof(model->row));
					}
					if (*model->mmap.cur == '\0') {
						csvtmt_close_mmap(model);
						csvtmt_append_rows_to_table(
							model->table_path,
							model->rows,
							false,
							error
						);
						if (error->error) {
							goto failed_to_append_rows;
						}
						csvtmt_clear_rows(model->rows);
						csvtmt_row_final(&model->row);
					} else {
						restore_save_index();
						csvtmt_row_final(&model->row);
						continue;
					}
				}
			} else {
				csvtmt_close_mmap(model);
				csvtmt_update_all(model, error);
				if (error->error) {
					goto failed_to_update_all;
				}
				csvtmt_clear_rows(model->rows);
				csvtmt_row_final(&model->row);
			}
		} break;
		case CSVTMT_OP_UPDATE_SET_BEG: {
			model->update_set_key_values_len = 0;
			model->mode = CSVTMT_MODE_UPDATE_SET;
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

		case CSVTMT_OP_SELECT_STMT_BEG: {
			// puts("select beg");
			cur_context = CSVTMT_OP_SELECT_STMT_BEG;

			if (model->mmap.fd == 0) {
				model->table_name = op->obj.select_stmt.table_name;
				model->column_names_len = 0;
				store_table_path(model, model->table_name);

				csvtmt_open_mmap_for_read_write(model, model->table_path, error);
				if (error->error) {
					goto failed_to_open_mmap;
				}				

				model->mmap.cur = (char *) csvtmt_header_read_from_string(&model->header, model->mmap.cur, error);
				if (error->error) {
					goto failed_to_read_header;
				}
			}

			if (*model->mmap.cur == '\0') {
				csvtmt_close_mmap(model);
				goto done;
			}

			model->skip = false;
			model->save_opcodes_index = model->opcodes_index;
			model->column_names_is_star = false;
			model->selected_columns_len = 0;
			model->column_names_len = 0;

			csvtmt_parse_row_from_mmap(model, error);
			if (error->error) {
				goto failed_to_parse_row;
			}
			if (csvtmt_is_deleted_row(&model->row)) {
				// puts("deleted row");
				model->opcodes_index = skip_to(
					model,
					opcodes,
					opcodes_len,
					CSVTMT_OP_SELECT_STMT_END,
					error
				);
				model->skip = true;
				csvtmt_row_final(&model->row);
				continue;
			}
		} break;
		case CSVTMT_OP_SELECT_STMT_END: {
			// puts("select end");
			cur_context = CSVTMT_OP_SELECT_STMT_END;

			if (model->skip) {
				csvtmt_row_final(&model->row);
				restore_save_index();
				continue;
			}
			if (model->stack_len) {
				CsvTomatoStackElem top;
				stack_top(top);
				if (top.kind != CSVTMT_STACK_ELEM_BOOL_VALUE) {
					// WHERE無し。全取得。
					// puts("where nashi 1");
					csvtmt_store_selected_columns(model, &model->row, error);
					if (error->error) {
						goto failed_to_store_selected_columns;
					}
					restore_save_index();
					goto ret_row;
				} else if (top.obj.bool_value.value) {
					// WHERE match
					// puts("match");
					csvtmt_store_selected_columns(model, &model->row, error);
					if (error->error) {
						goto failed_to_store_selected_columns;
					}
					restore_save_index();
					goto ret_row;
				} else {
					// WHERE not match
					// puts("where not match");
					csvtmt_row_final(&model->row);
					restore_save_index();
					continue;
				}
			} else {
				// WHERE無し。全取得。
				// puts("where nashi 2");
				csvtmt_store_selected_columns(model, &model->row, error);
				if (error->error) {
					goto failed_to_store_selected_columns;
				}
				restore_save_index();
				goto ret_row;
			}
		} break;

		case CSVTMT_OP_INSERT_STMT_BEG: {
			model->table_name = op->obj.insert_stmt.table_name;
			model->column_names_len = 0;
			model->values_len = 0;
			store_table_path(model, model->table_name);
		} break;
		case CSVTMT_OP_INSERT_STMT_END: {
			csvtmt_insert(model, error);
			if (error->error) {
				goto insert_error;
			}			
		} break;

		// DELETE 
		/*
			DELETE FROM users WHERE age = 123 AND id < 10;
			↓	
			op [
				delete_beg,
			    where_beg,
					ident, int, equal, 
			        ident, int, lt, 
			        and, 
                where_end, 
			    delete_end,
			] {
				delete_beg {
					if !opened {
						open_mmap 
						beg_index = i
					}
					row = read_row
					mode = FIRST
				}
				where_beg {
					mode = WHERE_MODE
				}
				push ident -> [ident]
				push int -> [ident, int]
				equal (assign) {
					int = pop <- [ident, int]
					ident = pop <- [ident]
					push (row[ident] == int) -> [bool]
				}
				push ident -> [bool, ident]
				push int -> [bool, ident, int]
				lt {
					int = pop <- [bool, ident, int]
					ident = pop <- [bool, ident]
					push (row[ident] < int) -> [bool, bool]
				}
				and {
					lhs = pop <- [bool, bool]
					rhs = pop <- [bool]
					push (lhs && rhs) -> [bool]
				}
				where_end {
				}
				delete_end {
					if stack_top is not bool {
						*head = '1' // all deletion
					} else {
						do_delete = pop <- [bool]
						if do_delete {
							*head = '1'
						}	
					}
				}
			}
		*/
		case CSVTMT_OP_DELETE_STMT_BEG: {
			if (model->mmap.fd == 0) {
				model->table_name = op->obj.delete_stmt.table_name;
				store_table_path(model, model->table_name);
				csvtmt_open_mmap_for_read_write(model, model->table_path, error);
				if (error->error) {
					goto failed_to_open_mmap;
				}
				
				model->mmap.cur = (char *) csvtmt_header_read_from_string(&model->header, model->mmap.cur, error);	
				if (error->error) {
					goto failed_to_read_header;
				}
			}

			model->mode = CSVTMT_MODE_FIRST;
			model->save_opcodes_index = model->opcodes_index;

			model->row_head = model->mmap.cur;
			csvtmt_parse_row_from_mmap(model, error);
			if (error->error) {
				goto failed_to_parse_row;
			}

		} break;
		case CSVTMT_OP_DELETE_STMT_END: {
			if (model->stack_len) {
				CsvTomatoStackElem top;
				stack_top(top);
				if (top.kind != CSVTMT_STACK_ELEM_BOOL_VALUE) {
					csvtmt_delete_row_head(model);
				} else if (top.obj.bool_value.value) {
					csvtmt_delete_row_head(model);
				}
			} else {
				csvtmt_delete_row_head(model);
			}
			if (*model->mmap.cur == '\0') {
				csvtmt_close_mmap(model);
			} else {
				model->opcodes_index = model->save_opcodes_index-1;
			}
			csvtmt_row_final(&model->row);
		} break;

		// WHERE
		case CSVTMT_OP_WHERE_BEG: {
			model->mode = CSVTMT_MODE_WHERE;
		} break;
		case CSVTMT_OP_WHERE_END: {
			model->mode = CSVTMT_MODE_FIRST;
		} break;

		// ASSIGN
		case CSVTMT_OP_ASSIGN: {
			CsvTomatoStackElem lhs, rhs;
			stack_pop(rhs);
			stack_pop(lhs);

			switch (lhs.kind) {
			default: goto invalid_elem_kind; break;
			case CSVTMT_STACK_ELEM_IDENT: {
				switch (rhs.kind) {
				default: goto invalid_elem_kind; break;
				case CSVTMT_STACK_ELEM_INT_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_BOOL_VALUE;
					int index = csvtmt_find_type_index(model, lhs.obj.ident.value);
					if (index == -1) {
						goto not_found_type_name;	
					}

					switch (model->mode) {
					default: goto invalid_mode; break;
					case CSVTMT_MODE_WHERE: {
						const char *col = model->row.columns[index];
						assert(model->row.len);
						assert(col);
						char num[CSVTMT_NUM_STR_SIZE];
						snprintf(num, sizeof num, "%ld", rhs.obj.int_value.value);
						elem.obj.bool_value.value = !strcmp(num, col);
						stack_push(elem);
					} break;
					case CSVTMT_MODE_UPDATE_SET: {
						elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE;
						elem.obj.key_value.key = lhs.obj.ident.value;
						elem.obj.key_value.value.kind = CSVTMT_VAL_INT;
						elem.obj.key_value.value.int_value = rhs.obj.int_value.value;
						stack_push(elem);
					} break;
					}
				} break;
				case CSVTMT_STACK_ELEM_DOUBLE_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_BOOL_VALUE;
					int index = csvtmt_find_type_index(model, lhs.obj.ident.value);
					if (index == -1) {
						goto not_found_type_name;	
					}

					switch (model->mode) {
					default: goto invalid_mode; break;
					case CSVTMT_MODE_WHERE: {
						const char *col = model->row.columns[index];
						char num[CSVTMT_NUM_STR_SIZE];
						snprintf(num, sizeof num, "%f", rhs.obj.double_value.value);
						elem.obj.bool_value.value = !strcmp(num, col);
						stack_push(elem);
					} break;
					case CSVTMT_MODE_UPDATE_SET: {
						elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE;
						elem.obj.key_value.key = lhs.obj.ident.value;
						elem.obj.key_value.value.kind = CSVTMT_VAL_DOUBLE;
						elem.obj.key_value.value.double_value = rhs.obj.double_value.value;
						stack_push(elem);
					} break;
					}
				} break;
				case CSVTMT_STACK_ELEM_STRING_VALUE: {
					CsvTomatoStackElem elem = {0};
					elem.kind = CSVTMT_STACK_ELEM_BOOL_VALUE;
					int index = csvtmt_find_type_index(model, lhs.obj.ident.value);
					if (index == -1) {
						goto not_found_type_name;	
					}

					switch (model->mode) {
					default: goto invalid_mode; break;
					case CSVTMT_MODE_WHERE: {
						const char *col = model->row.columns[index];
						elem.obj.bool_value.value = !strcmp(rhs.obj.string_value.value, col);
						stack_push(elem);
					} break;
					case CSVTMT_MODE_UPDATE_SET: {
						elem.kind = CSVTMT_STACK_ELEM_KEY_VALUE;
						elem.obj.key_value.key = lhs.obj.ident.value;
						elem.obj.key_value.value.kind = CSVTMT_VAL_STRING;
						elem.obj.key_value.value.string_value = rhs.obj.string_value.value;
						stack_push(elem);
					} break;
					}
				} break;
				}
			} break;
			}
		} break;

		case CSVTMT_OP_IDENT: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_IDENT;
			elem.obj.ident.value = op->obj.ident.value;
			stack_push(elem);
		} break;
		case CSVTMT_OP_COLUMN_NAMES_BEG: {
			stack_push_kind(CSVTMT_STACK_ELEM_COLUMN_NAMES_BEG);
		} break;
		case CSVTMT_OP_COLUMN_NAMES_END: {
			const char *cols[CSVTMT_CSV_COLS_SIZE];
			size_t cols_len = 0;

			while (model->stack_len) {
				stack_pop(pop);
				if (pop.kind == CSVTMT_STACK_ELEM_COLUMN_NAMES_BEG) {
					break;
				}
				switch (pop.kind) {
				default: goto invalid_elem_kind; break;
				case CSVTMT_STACK_ELEM_STAR:
					// puts("star");
					if (model->column_names_is_star) {
						goto multiple_star_error;
					}
					model->column_names_is_star = true;
					break;
				case CSVTMT_STACK_ELEM_STRING_VALUE:
					if (model->column_names_is_star) {
						goto inavlid_column_names_with_star;
					}
					check_array(cols);
					cols[cols_len++] = pop.obj.string_value.value;
					break;
				}
			}

			// 逆順に格納されている配列を順列に直す。
			for (ssize_t i = cols_len-1; i >= 0; i--) {
				check_array(model->column_names);
				model->column_names[model->column_names_len++] = cols[i];	
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
			if (model->values_len >= csvtmt_numof(model->values)) {
				goto array_overflow;
			}

			CsvTomatoValues *dst_values = &model->values[model->values_len];
			CsvTomatoValue tmp_value_array[CSVTMT_VALUES_ARRAY_SIZE] = {0};
			size_t tmp_value_array_len = 0;

			while (model->stack_len) {
				stack_pop(pop);

				if (pop.kind == CSVTMT_STACK_ELEM_VALUES_BEG) {
					break;
				}
				if (tmp_value_array_len >= csvtmt_numof(tmp_value_array)) {
					goto array_overflow;
				}

				CsvTomatoValue *value = &tmp_value_array[tmp_value_array_len++];

				switch (pop.kind) {
				default: goto invalid_elem_kind; break;
				case CSVTMT_STACK_ELEM_INT_VALUE:
					value->kind = CSVTMT_VAL_INT;
					value->int_value = pop.obj.int_value.value;
					break;
				case CSVTMT_STACK_ELEM_DOUBLE_VALUE:
					value->kind = CSVTMT_VAL_DOUBLE;
					value->double_value = pop.obj.double_value.value;
					break;
				case CSVTMT_STACK_ELEM_STRING_VALUE:
					value->kind = CSVTMT_VAL_STRING;
					value->string_value = pop.obj.string_value.value;
					break;
				}

				// printf("values->len %ld\n", values->len);
			}

			// 逆順に格納されている配列を順列に直す。
			for (ssize_t i = tmp_value_array_len-1; i >= 0; i--) {
				dst_values->values[dst_values->len++] = tmp_value_array[i];	
			}

			model->values_len++;
			// printf("values_len %ld\n", model->values_len);
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
		case CSVTMT_OP_DOUBLE_VALUE: {
			CsvTomatoStackElem elem = {0};
			elem.kind = CSVTMT_STACK_ELEM_DOUBLE_VALUE;
			elem.obj.double_value.value = op->obj.double_value.value;
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
					goto table_already_exists;
				}
			} else {
				model->do_create_table = true;
				// 論理削除カラム（予約カラム）をヘッダにセット。
				csvtmt_str_append(buf, CSVTMT_COL_MODE);
				csvtmt_str_append(buf, ",");
			}
		} break;
		case CSVTMT_OP_CREATE_TABLE_STMT_END: {
			if (!model->do_create_table) {
				model->opcodes_index++;
				continue;
			}

			// buf stored column def strings
			csvtmt_str_pop_back(buf); // last ,

			errno = 0;
			FILE *fp = fopen(model->table_path, "w");
			if (!fp) {
				goto failed_to_open_table;
			}

			fprintf(fp, "%s\n", buf->str);

			fclose(fp);
			csvtmt_str_clear(buf);

			model->do_create_table = false;
		} break;
		case CSVTMT_OP_COLUMN_DEF: {
			if (!model->do_create_table) {
				model->opcodes_index++;
				continue;
			}

			const char *column_name = op->obj.column_def.column_name;
			csvtmt_str_append(buf, column_name);

			const CsvTomatoTokenKind type_name = op->obj.column_def.type_name;
			switch (type_name) {
			default: 
				goto invalid_type_name;
				break;
			case CSVTMT_TK_INTEGER: csvtmt_str_append(buf, " INTEGER"); break;
			case CSVTMT_TK_TEXT: csvtmt_str_append(buf, " TEXT"); break;
			}

			if (op->obj.column_def.primary &&
				op->obj.column_def.key) {
				csvtmt_str_append(buf, " PRIMARY KEY");
			}
			if (op->obj.column_def.autoincrement) {
				csvtmt_str_append(buf, " AUTOINCREMENT");
			}
			if (op->obj.column_def.not_ &&
				op->obj.column_def.null) {
				csvtmt_str_append(buf, " NOT NULL");
			}

			csvtmt_str_push_back(buf, ',');
		} break;
		}

		model->opcodes_index++;
	}

	cleanup();
	return result;

done:
	cleanup();
	return CSVTMT_DONE;
ret_row:
	cleanup();
	return CSVTMT_ROW;
invalid_context:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid context");
	cleanup();
	return CSVTMT_ERROR;
not_found_keys:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "not found keys");
	cleanup();
	return CSVTMT_ERROR;
failed_to_store_selected_columns:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to store selected columns");
	cleanup();
	return CSVTMT_ERROR;
failed_to_push_row:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to push row");
	cleanup();
	return CSVTMT_ERROR;
failed_to_replace_row:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to replace row");
	cleanup();
	return CSVTMT_ERROR;
failed_to_update_all:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to update all");
	cleanup();
	return CSVTMT_ERROR;
failed_to_store_col_infos:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to store column infos");
	cleanup();
	return CSVTMT_ERROR;
failed_to_allocate_rows:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to allocate rows");
	cleanup();
	return CSVTMT_ERROR;
failed_to_append_rows:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to append rows");
	cleanup();
	return CSVTMT_ERROR;
invalid_mode:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid mode");
	cleanup();
	return CSVTMT_ERROR;
not_found_type_name:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "not found type name");
	cleanup();
	return CSVTMT_ERROR;
failed_to_parse_row:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to parse row");
	cleanup();
	return CSVTMT_ERROR;
failed_to_read_header:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "failed to read header");
	cleanup();
	return CSVTMT_ERROR;
failed_to_open_mmap:
	csvtmt_error_push(error, CSVTMT_ERR_FILE_IO, "failed to open mmap");
	cleanup();
	return CSVTMT_ERROR;
inavlid_column_names_with_star:
	csvtmt_error_push(error, CSVTMT_ERR_SYNTAX, "invalid column names. found star");
	cleanup();
	return CSVTMT_ERROR;
multiple_star_error:
	csvtmt_error_push(error, CSVTMT_ERR_SYNTAX, "found multiple star");
	cleanup();
	return CSVTMT_ERROR;
failed_to_open_table:
	csvtmt_error_push(error, CSVTMT_ERR_FILE_IO, "failed to open table %s: %s", model->table_path, strerror(errno));
	cleanup();
	return CSVTMT_ERROR;
failed_to_allocate_buffer:
	csvtmt_error_push(error, CSVTMT_ERR_MEM, "failed to allocate buffer: %s", strerror(errno));
	cleanup();
	return CSVTMT_ERROR;
found_place_holder:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "found place holder");
	cleanup();
	return CSVTMT_ERROR;
table_already_exists:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "table %s already exists", model->table_name);
	cleanup();
	return CSVTMT_ERROR;
invalid_type_name:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid type name");
	cleanup();
	return CSVTMT_ERROR;
stack_overflow:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "stack overflow");
	cleanup();
	return CSVTMT_ERROR;
stack_underflow:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "stack underflow");
	cleanup();
	return CSVTMT_ERROR;
invalid_stack_elem_kind:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid stack element kind");
	cleanup();
	return CSVTMT_ERROR;
insert_error:
	cleanup();
	return CSVTMT_ERROR;	
array_overflow:
	csvtmt_error_push(error, CSVTMT_ERR_BUF_OVERFLOW, "array overflow");
	cleanup();
	return CSVTMT_ERROR;
invalid_elem_kind:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid stack element kind");
	cleanup();
	return CSVTMT_ERROR;
invalid_op_kind:
	csvtmt_error_push(error, CSVTMT_ERR_EXEC, "invalid op kind");
	cleanup();
	return CSVTMT_ERROR;
}
