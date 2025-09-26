#include <csvtomato.h>

int main(void) {
	char cmd[1024];
	const char *db_dir = "test_db";

	for (;;) {
		printf("> ");
		fflush(stdout);

		if (!fgets(cmd, sizeof cmd, stdin)) {
			break;
		}

		csvtmt_quick_exec(db_dir, cmd);
	}

	return 0;
}
