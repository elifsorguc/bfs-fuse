all: make_bfs bfs

make_bfs: make_bfs.c
	gcc -O2 -Wall -o make_bfs make_bfs.c

bfs: bfs.c
	gcc -O2 -Wall -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=31 `pkg-config --cflags --libs fuse3` -o bfs bfs.c -lfuse3

clean:
	rm -f make_bfs bfs *.o *~
