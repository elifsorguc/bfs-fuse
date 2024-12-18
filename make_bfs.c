/*
    This initializes (i.e., formats) the disk with the BFS file system.
    The on-disk data structures (superblock, bitmap, inode map, inode table, root directory)
    will be created and initialized on the disk. Initially, there will be no file on the
    disk. Therefore, when we type `ls` in the root directory of the BFS file system,
    only two entries should be listed: "." and "..".
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

// Superblock Structure
typedef struct
{
    int total_blocks;
    int block_size;
    int inode_count;
    int root_dir_block;
} Superblock;

// Directory Entry Structure
typedef struct
{
    char name[FILENAME_LEN];
    int inode_num;
} DirectoryEntry;

// Function Prototypes
int read_block(int fd_disk, void *block, int k);
int write_block(int fd_disk, void *block, int k);

// Reads a block from the disk
int read_block(int fd_disk, void *block, int k)
{
    int offset = k * BLOCK_SIZE;
    lseek(fd_disk, offset, SEEK_SET);
    int n = read(fd_disk, block, BLOCK_SIZE);
    if (n != BLOCK_SIZE)
    {
        printf("Read error at block %d\n", k);
        return -1;
    }
    return 0;
}

// Writes a block to the disk
int write_block(int fd_disk, void *block, int k)
{
    int offset = k * BLOCK_SIZE;
    lseek(fd_disk, offset, SEEK_SET);
    int n = write(fd_disk, block, BLOCK_SIZE);
    if (n != BLOCK_SIZE)
    {
        printf("Write error at block %d\n", k);
        return -1;
    }
    return 0;
}

int main()
{
    int fd = open("disk1", O_CREAT | O_RDWR, 0666);
    if (fd < 0)
    {
        perror("Failed to open disk file");
        return 1;
    }

    char buffer[BLOCK_SIZE] = {0};

    // Superblock Initialization
    Superblock sb = {TOTAL_BLOCKS, BLOCK_SIZE, MAX_FILES, BITMAP_BLOCKS + INODE_MAP_BLOCK + 1};
    memcpy(buffer, &sb, sizeof(Superblock));
    if (write_block(fd, buffer, 0) != 0)
    {
        printf("Error initializing superblock\n");
        close(fd);
        return 1;
    }

    // Bitmap Initialization (blocks 1-2)
    memset(buffer, 0xFF, BLOCK_SIZE); // Mark system blocks as used
    buffer[0] = 0x03;                 // Mark superblock and bitmap blocks as used
    if (write_block(fd, buffer, 1) != 0 || write_block(fd, buffer, 2) != 0)
    {
        printf("Error initializing bitmap\n");
        close(fd);
        return 1;
    }

    // Inode Map Initialization
    memset(buffer, 0, BLOCK_SIZE);
    buffer[0] = 1; // Mark root directory inode as used
    if (write_block(fd, buffer, 3) != 0)
    {
        printf("Error initializing inode map\n");
        close(fd);
        return 1;
    }

    // Inode Table Initialization (blocks 4-11)
    for (int i = 4; i < INODE_TABLE_BLOCKS + 4; i++)
    {
        memset(buffer, 0, BLOCK_SIZE);
        if (write_block(fd, buffer, i) != 0)
        {
            printf("Error initializing inode table\n");
            close(fd);
            return 1;
        }
    }

    // Root Directory Initialization
    DirectoryEntry root_dir[2] = {{".", 1}, {"..", 1}};
    memset(buffer, 0, BLOCK_SIZE);
    memcpy(buffer, root_dir, sizeof(root_dir));
    if (write_block(fd, buffer, BITMAP_BLOCKS + INODE_MAP_BLOCK + 1) != 0)
    {
        printf("Error initializing root directory\n");
        close(fd);
        return 1;
    }

    // Zero out all remaining blocks (free data blocks)
    memset(buffer, 0, BLOCK_SIZE);
    for (int i = BITMAP_BLOCKS + INODE_MAP_BLOCK + ROOT_DIR_BLOCKS + 1; i < TOTAL_BLOCKS; i++)
    {
        if (write_block(fd, buffer, i) != 0)
        {
            printf("Error clearing block %d\n", i);
            close(fd);
            return 1;
        }
    }

    printf("Disk Initialized Successfully.\n");
    close(fd);
    return 0;
}
