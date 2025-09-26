#include <csvtomato.h>

int main(int argc, char *argv[]) {
	const char *db_dir = "test_db";
	if (argc >= 2) {
		db_dir = argv[1];
	}

	if (!csvtmt_file_exists(db_dir)) {
		fprintf(stderr, "can't open database. %s does not exists.\n", db_dir);
		return 1;
	}

	char cmd[1024];

	for (;;) {
		printf("%s > ", db_dir);
		fflush(stdout);

		if (!fgets(cmd, sizeof cmd, stdin)) {
			break;
		}

		csvtmt_quick_exec(db_dir, cmd);
	}

	return 0;
}
