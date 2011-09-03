CFLAGS =  -g
OBJS = btree.o main.o

btree: $(OBJS)
	$(CC) -o $@ $(OBJS)  -lrt
clean:	
	rm -rf *.o  btree
cleanall:	
	rm -rf *.o *.db  *.idx btree
