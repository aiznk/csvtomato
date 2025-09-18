CC := gcc
PROG_FLAGS := -Wall -g -O0 -std=c11 -pedantic-errors -fsanitize=address
SO_FLAGS := -O2 -std=c11 -pedantic-errors
SO := csvtomato.so
PROG := test.out
SRCS := $(wildcard *.c)
OBJS := $(SRCS:.c=.o)

all: $(PROG) $(SO)

$(OBJS): csvtomato.h

$(SO): $(SRCS)
	$(CC) $(SO_FLAGS) -o $@ -shared $^

$(PROG): $(SRCS) test/test.c
	$(CC) $(PROG_FLAGS) -o $@ $^

.PHONY: clean test
clean:
	rm -f *.out *.so *.o

test:
	make clean
	make
	./test.out 
