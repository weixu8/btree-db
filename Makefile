CFLAGS =  -g  -std=c99 -W -Wall -Werror
OBJS = btree.o test.o

test: $(OBJS)
	$(CC) -o $@ $(OBJS)
clean:	
	rm -rf *.o *.db  *.idx test
