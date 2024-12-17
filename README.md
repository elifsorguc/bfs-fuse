# bfs-fuse

DOC REPORT FILE:
https://docs.google.com/document/d/11rjVarKwEj1IegN-UA2CScY90YDtCDG7aqtD3h7HCWY/edit?usp=sharing

bfs.c:
The bfs program, hence the file system, will run in user-space.
It will store file and meta-data in a regular Linux file.
This Linux file will be the disk of the file system.

make_bfc.c:
This initializes (i.e., format) the disk with the BFS file system. The on-disk data
structures (superblock, bitmap, inode-map, inode table, root directory) will
be created and initialized on the disk. Initially there will be no file on the
disk. Therefore, initially, when we type ls in the root directory of the BFS file
system, only two entries should be listed: “.” and “..”.
