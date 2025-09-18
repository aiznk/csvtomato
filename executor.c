#include "csvtomato.h"

CsvTomatoExecutor *
csvtmt_executor_new(
	const char *db_dir, 
	CsvTomatoError *error
) {
	errno = 0;
	CsvTomatoExecutor *self = calloc(1, sizeof(*self));
	if (!self) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate memory: %s", strerror(errno));
		return NULL;
	}

	snprintf(self->db_dir, sizeof self->db_dir, "%s", db_dir);

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
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoError *error
) {
	CsvTomatoString *buf = csvtmt_str_new();
	if (!buf) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate buffer: %s", strerror(errno));
		return;
	}

	const char *table_name = NULL;
	char table_path[CSVTMT_PATH_SIZE*2];
	bool do_create_table = false;

	for (size_t i = 0; i < opcodes_len; i++) {
		const CsvTomatoOpcodeElem *op = &opcodes[i];

		switch (op->kind) {
		case CSVTMT_OP_NONE: break;
		case CSVTMT_OP_CREATE_TABLE_BEG: {
			table_name = op->obj.create_table_stmt.table_name;
			assert(table_name);
			bool if_not_exists = op->obj.create_table_stmt.if_not_exists;

			snprintf(table_path, sizeof table_path, "%s/%s.csv", self->db_dir, table_name);

			if (csvtmt_file_exists(table_path)) {
				do_create_table = false;
				if (!if_not_exists) {
					csvtmt_error_format(error, CSVTMT_ERR_EXEC, "table %s already exists", table_name);
					return;
				}
			} else {
				do_create_table = true;
			}
		} break;
		case CSVTMT_OP_CREATE_TABLE_END: {
			if (!do_create_table) {
				continue;
			}

			csvtmt_str_pop_back(buf);
			printf("buf[%s]\n", buf->str);

			errno = 0;
			FILE *fp = fopen(table_path, "w");
			if (!fp) {
				csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table %s: %s", table_name, strerror(errno));
				return;
			}

			fprintf(fp, "%s\n", buf->str);

			fclose(fp);
			csvtmt_str_clear(buf);

			do_create_table = false;
		} break;
		case CSVTMT_OP_COLUMN_DEF: {
			if (!do_create_table) {
				continue;
			}

			const char *column_name = op->obj.column_def.column_name;
			csvtmt_str_append(buf, column_name);

			const CsvTomatoTokenKind type_name = op->obj.column_def.type_name;
			switch (type_name) {
			default: 
				csvtmt_error_format(error, CSVTMT_ERR_EXEC, "invalid type name: %d", type_name);
				return;
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
	}

	csvtmt_str_del(buf);
}
