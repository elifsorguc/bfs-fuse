#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <stdint.h>

#define BLOCK_SIZE 4096
#define TOTAL_BLOCKS 4096
#define BITMAP_BLOCKS 2
#define INODE_MAP_BLOCK 3
#define INODE_TABLE_BLOCKS 8
#define ROOT_DIR_BLOCKS 2
#define MAX_FILES 128
#define FILENAME_LEN 48
#define INODE_TABLE_START 4

// Superblock structure
typedef struct
{
    int total_blocks;   // Total number of blocks
    int block_size;     // Block size in bytes
    int inode_count;    // Total number of inodes
    int root_dir_block; // Start block of the root directory
} Superblock;

// Directory Entry structure
typedef struct
{
    char name[FILENAME_LEN]; // File name
    int inode_num;           // Inode number
} DirectoryEntry;

// Inode structure
typedef struct
{
    int size;              // File size in bytes
    int block_pointers[8]; // Direct block pointers
    int indirect_pointer;  // Indirect block pointer
    time_t creation_time;
    time_t modification_time;
    mode_t permissions;
    int ref_count; // Reference count for links
} Inode;

// Utility Function to write a block to disk
int write_block(int fd, void *data, int block_num)
{
    if (lseek(fd, block_num * BLOCK_SIZE, SEEK_SET) == -1)
    {
        perror("WRITE_BLOCK ERROR: lseek failed");
        return -1;
    }
    ssize_t bytes_written = write(fd, data, BLOCK_SIZE);
    if (bytes_written != BLOCK_SIZE)
    {
        perror("WRITE_BLOCK ERROR: write failed");
        return -1;
    }
    return 0;
}

int main()
{
    int fd = open("disk1", O_CREAT | O_RDWR, 0666);
    if (fd < 0)
    {
        perror("Failed to create disk file");
        return 1;
    }

    char buffer[BLOCK_SIZE];
    memset(buffer, 0, BLOCK_SIZE);

    // 1. Initialize the Superblock
    Superblock sb;
    sb.total_blocks = TOTAL_BLOCKS;
    sb.block_size = BLOCK_SIZE;
    sb.inode_count = MAX_FILES;
    sb.root_dir_block = 12; // As per specifications

    memcpy(buffer, &sb, sizeof(Superblock));
    if (write_block(fd, buffer, 0) != 0)
    {
        close(fd);
        return 1;
    }
    printf("Superblock initialized.\n");

    // 2. Initialize the Bitmap (blocks 1 and 2)
    memset(buffer, 0, BLOCK_SIZE);
    // Mark blocks 0-13 as used (0-1: superblock and bitmap, 3: inode map, 4-11: inode table, 12-13: root dir)
    for (int i = 0; i <= 13; i++)
    {
        buffer[i / 8] |= (1 << (i % 8));
    }
    if (write_block(fd, buffer, 1) != 0 || write_block(fd, buffer, 2) != 0)
    {
        close(fd);
        return 1;
    }
    printf("Bitmap initialized.\n");

    // 3. Initialize the Inode Map (block 3)
    memset(buffer, 0, BLOCK_SIZE);
    buffer[0] |= 0x01; // Mark inode 1 as used (root directory)
    if (write_block(fd, buffer, INODE_MAP_BLOCK) != 0)
    {
        close(fd);
        return 1;
    }
    printf("Inode map initialized.\n");

    // 4. Initialize the Inode Table (blocks 4-11)
    Inode inodes[MAX_FILES];
    memset(inodes, 0, sizeof(inodes));

    // Initialize inode 1 (root directory)
    inodes[0].size = 0;
    inodes[0].block_pointers[0] = 12; // Block 12
    inodes[0].block_pointers[1] = 13; // Block 13
    inodes[0].indirect_pointer = 0;
    inodes[0].creation_time = inodes[0].modification_time = time(NULL);
    inodes[0].permissions = 0755;
    inodes[0].ref_count = 2; // "." and ".."

    // Write inodes to inode table blocks
    for (int block = 0; block < INODE_TABLE_BLOCKS; block++)
    {
        memset(buffer, 0, BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE / sizeof(Inode); i++)
        {
            int inode_num = block * (BLOCK_SIZE / sizeof(Inode)) + i;
            if (inode_num < MAX_FILES)
            {
                memcpy(buffer + i * sizeof(Inode), &inodes[inode_num], sizeof(Inode));
            }
        }
        if (write_block(fd, buffer, INODE_TABLE_START + block) != 0)
        {
            close(fd);
            return 1;
        }
    }
    printf("Inode table initialized.\n");

    // 5. Initialize the Root Directory (blocks 12 and 13)
    DirectoryEntry root_dir[2] = {{".", 1}, {"..", 1}};
    memset(buffer, 0, BLOCK_SIZE);
    memcpy(buffer, root_dir, sizeof(root_dir));
    if (write_block(fd, buffer, sb.root_dir_block) != 0)
    {
        close(fd);
        return 1;
    }
    printf("Root directory initialized.\n");

    // 6. Clear all remaining blocks (blocks 14 onwards)
    memset(buffer, 0, BLOCK_SIZE);
    for (int i = sb.root_dir_block + ROOT_DIR_BLOCKS; i < TOTAL_BLOCKS; i++)
    {
        if (write_block(fd, buffer, i) != 0)
        {
            close(fd);
            return 1;
        }
    }
    printf("Disk blocks cleared.\n");

    printf("Disk initialized successfully.\n");
    close(fd);
    return 0;
}
