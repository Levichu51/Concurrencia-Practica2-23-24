CFLAGS=-g -Wall -pthread
OBJS=compress.o chunk_archive.o options.o queue.o comp.o
LIBS=-lz
CC=gcc

all: comp

comp: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIBS)

clean:
	rm -f *.o comp
