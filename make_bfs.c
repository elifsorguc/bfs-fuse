/*
This initializes (i.e., format) the disk with the BFS file system. The on-disk data
structures (superblock, bitmap, inode-map, inode table, root directory) will
be created and initialized on the disk. Initially there will be no file on the
disk. Therefore, initially, when we type ls in the root directory of the BFS file
system, only two entries should be listed: “.” and “..”.
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define BLOCK_SIZE 4096
#define TOTAL_BLOCKS 4096
#define BITMAP_BLOCKS 2
#define INODE_MAP_BLOCK 1
#define INODE_TABLE_BLOCKS 8
#define ROOT_DIR_BLOCKS 2
#define MAX_FILES 128
#define FILENAME_LEN 48

typedef struct
{
    int total_blocks;
    int block_size;
    int inode_count;
    int root_dir_block;
} Superblock;

typedef struct
{
    char name[FILENAME_LEN];
    int inode_num;
} DirectoryEntry;

void write_block(int fd, int block_num, void *data)
{
    lseek(fd, block_num * BLOCK_SIZE, SEEK_SET);
    ssize_t bytes_written = write(fd, data, BLOCK_SIZE);
    if (bytes_written != BLOCK_SIZE)
    {
        perror("Failed to write block");
        exit(EXIT_FAILURE);
    }
}

int main()
{
    int fd = open("disk1", O_CREAT | O_RDWR, 0666);
    char buffer[BLOCK_SIZE] = {0};

    // Superblock Initialization
    Superblock sb = {TOTAL_BLOCKS, BLOCK_SIZE, MAX_FILES, BITMAP_BLOCKS + 1};
    memcpy(buffer, &sb, sizeof(Superblock));
    write_block(fd, 0, buffer);

    // Bitmap Initialization (blocks 1-2)
    memset(buffer, 0xFF, BLOCK_SIZE);
    buffer[0] = 0x03; // Mark system blocks as used
    write_block(fd, 1, buffer);
    write_block(fd, 2, buffer);

    // Inode Map Initialization
    memset(buffer, 0, BLOCK_SIZE);
    buffer[0] = 1; // Root directory inode
    write_block(fd, 3, buffer);

    // Root Directory Initialization
    DirectoryEntry root_dir[2] = {{".", 1}, {"..", 1}};
    memcpy(buffer, root_dir, sizeof(root_dir));
    write_block(fd, BITMAP_BLOCKS + INODE_MAP_BLOCK + 1, buffer);

    printf("Disk Initialized Successfully.\n");
    close(fd);
    return 0;
}
