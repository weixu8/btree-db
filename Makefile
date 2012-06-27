CFLAGS =  -g
OBJS = btree.o test.o

test: $(OBJS)
	$(CC) -o $@ $(OBJS)
clean:	
	rm -rf *.o *.db  *.idx test
