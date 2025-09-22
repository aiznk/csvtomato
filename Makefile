CC := gcc
PROG_FLAGS_MEM := -I. -Wall -g -O0 -std=c11 -pedantic-errors -fsanitize=address -Wformat-truncation=0
PROG_FLAGS := -I. -Wall -g -O0 -std=c11 -pedantic-errors
SO_FLAGS := -O2 -std=c11 -pedantic-errors
SO := csvtomato.so
TEST_PROG := test.out
SHELL_PROG := sh.out
SRCS := $(wildcard src/*.c)
OBJS := $(SRCS:src/.c=src/.o)

all: $(TEST_PROG) $(SHELL_PROG)

$(OBJS): csvtomato.h

$(SO): $(SRCS)
	$(CC) $(SO_FLAGS) -o $@ -shared $^

$(TEST_PROG): $(SRCS) test/test.c
	$(CC) $(PROG_FLAGS_MEM) -o $@ $^

$(SHELL_PROG): $(SRCS) shell.c
	$(CC) $(PROG_FLAGS_MEM) -o $@ $^

.PHONY: clean test
clean:
	rm -f *.out *.so *.o src/*.o

test:
	make clean
	make
	./test.out 
