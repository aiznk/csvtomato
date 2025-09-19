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

typedef enum {
	MODE_NONE,
	MODE_COLUMN_NAMES,
	MODE_VALUES,
} Mode;

typedef enum {
	VAL_NONE,
	VAL_INT,
	VAL_FLOAT,
	VAL_STRING,
} ValueKind;

typedef struct {
	ValueKind kind;
	int64_t int_value;
	double float_value;
	const char *string_value;
} Value;

typedef struct {
	char type_name[CSVTMT_TYPE_NAME_SIZE];
	char type_def[CSVTMT_TYPE_DEF_SIZE];
	size_t index;
} Type;

typedef struct {
	Type types[CSVTMT_TYPES_ARRAY_SIZE];
	size_t types_len;
} Header;

void
read_header(Header *self, const char *table_path, CsvTomatoError *error) {
	errno = 0;
	FILE *fp = fopen(table_path, "r");
	if (!fp) {
		csvtmt_error_format(error, CSVTMT_ERR_FILE_IO, "failed to open table: %s", strerror(errno));
		return;
	}

	CsvTomatoCsvLine csv_line = {0};
	csvtmt_csvline_parse_stream(&csv_line, fp, error);
	if (error->error) {
		fclose(fp);
		return;
	}

	csvtmt_csvline_destroy(&csv_line);
	fclose(fp);
}

void
csvtmt_executor_exec(
	CsvTomatoExecutor *self,
	const CsvTomatoOpcodeElem *opcodes,
	size_t opcodes_len,
	CsvTomatoError *error
) {
	#define check_array(IDENT) {\
		if (IDENT ## _len >= csvtmt_numof(IDENT)) {\
			csvtmt_error_format(error, CSVTMT_ERR_BUF_OVERFLOW, "array overflow");\
			goto fail;\
		}\
	}\

	CsvTomatoString *buf = csvtmt_str_new();
	if (!buf) {
		csvtmt_error_format(error, CSVTMT_ERR_MEM, "failed to allocate buffer: %s", strerror(errno));
		return;
	}

	const char *table_name = NULL;
	char table_path[CSVTMT_PATH_SIZE*2];
	bool do_create_table = false;
	Mode mode = MODE_NONE;
	const char *column_names[CSVTMT_COLUMN_NAMES_ARRAY_SIZE] = {0};
	size_t column_names_len = 0;
	Value values[CSVTMT_VALUES_ARRAY_SIZE] = {0};
	size_t values_len = 0;
	Header header = {0};

	for (size_t i = 0; i < opcodes_len; i++) {
		const CsvTomatoOpcodeElem *op = &opcodes[i];

		switch (op->kind) {
		case CSVTMT_OP_NONE: break;
		case CSVTMT_OP_INSERT_STMT_BEG: {
			table_name = op->obj.insert_stmt.table_name;
		} break;
		case CSVTMT_OP_INSERT_STMT_END: {
			read_header(&header, table_path, error);
			if (error->error) {
				goto fail;
			}
		} break;
		case CSVTMT_OP_COLUMN_NAMES_BEG: {
			column_names_len = 0;
			mode = MODE_COLUMN_NAMES;
		} break;
		case CSVTMT_OP_COLUMN_NAMES_END: {
			mode = MODE_NONE;
		} break;
		case CSVTMT_OP_VALUES_BEG: {
			values_len = 0;
			mode = MODE_VALUES;
		} break;
		case CSVTMT_OP_VALUES_END: {
			mode = MODE_NONE;
		} break;
		case CSVTMT_OP_STRING_VALUE: {
			switch (mode) {
			case MODE_NONE: break;
			case MODE_COLUMN_NAMES:
				check_array(column_names);
				column_names[column_names_len++] = op->obj.string_value.value;
				break;
			case MODE_VALUES:
				check_array(values);
				values[values_len].kind = VAL_STRING;
				values[values_len].string_value = op->obj.string_value.value;
				values_len++;
				break;
			}
		} break;
		case CSVTMT_OP_INT_VALUE: {
			switch (mode) {
			default: break;
			case MODE_VALUES:
				check_array(values);
				values[values_len].kind = VAL_INT;
				values[values_len].int_value = op->obj.int_value.value;
				values_len++;				
				break;
			}
		} break;
		case CSVTMT_OP_FLOAT_VALUE: {
			switch (mode) {
			default: break;
			case MODE_VALUES:
				check_array(values);
				values[values_len].kind = VAL_FLOAT;
				values[values_len].float_value = op->obj.float_value.value;
				values_len++;				
				break;
			}
		} break;
		case CSVTMT_OP_CREATE_TABLE_STMT_BEG: {
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
		case CSVTMT_OP_CREATE_TABLE_STMT_END: {
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

fail:
	csvtmt_str_del(buf);
}
