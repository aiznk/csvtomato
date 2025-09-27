CC := gcc
PROG_FLAGS_MEM := -I. -Wall -g -O0 -std=c11 -pedantic-errors -fsanitize=address -Wformat-truncation=0 -Wno-unused-result
PROG_FLAGS := -I. -Wall -g -O0 -std=c11 -pedantic-errors -Wformat-truncation=0 -Wno-unused-result
SO_FLAGS := -I. -fPIC -O2 -std=c11 -pedantic-errors -Wformat-truncation=0 -Wno-unused-result
SO := csvtomato.so
TEST_PROG := test.out
SHELL_PROG := csvtomato.out
SRCS := $(wildcard src/*.c)
OBJS := $(SRCS:src/.c=src/.o)

all: $(TEST_PROG) $(SHELL_PROG) $(SO)

$(OBJS): csvtomato.h

$(SO): $(OBJS)
	$(CC) $(SO_FLAGS) -o $@ -shared $^

$(TEST_PROG): $(SRCS) test.c
	$(CC) $(PROG_FLAGS_MEM) -o $@ $^

$(SHELL_PROG): $(SRCS) shell.c
	$(CC) $(PROG_FLAGS_MEM) -o $@ $^

.PHONY: clean t d
clean:
	rm -f *.out *.so *.o src/*.o

t:
	make clean
	make
	./test.out 

d:
	make clean
	make
	gdb ./test.out
