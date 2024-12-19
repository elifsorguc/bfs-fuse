/*
 * BFS Disk Formatter (make_bfs.c)
 * This program initializes the simulated disk file with the BFS structure.
 * It sets up the superblock, bitmap, inode map, inode table, and root directory.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define BLOCK_SIZE 4096           // Block size (4 KB)
#define TOTAL_BLOCKS 4096         // Total number of blocks (16 MB disk)
#define BITMAP_BLOCKS 2           // Number of blocks used for the bitmap
#define INODE_MAP_BLOCK 1         // Block used for the inode map
#define INODE_TABLE_BLOCKS 8      // Number of blocks used for the inode table
#define ROOT_DIR_BLOCKS 2         // Number of blocks used for the root directory
#define FILENAME_LEN 48           // Maximum filename length (including null terminator)
#define MAX_FILES 128             // Maximum number of files

typedef struct {
    int total_blocks;             // Total number of blocks
    int block_size;               // Block size in bytes
    int inode_count;              // Total number of inodes
    int root_dir_block;           // Start block of the root directory
} Superblock;

typedef struct {
    char name[FILENAME_LEN];      // Name of the file
    int inode_num;                // Inode number
} DirectoryEntry;

// Function to write a block to the disk
void write_block(int fd, int block_num, void *data) {
    lseek(fd, block_num * BLOCK_SIZE, SEEK_SET);
    ssize_t bytes_written = write(fd, data, BLOCK_SIZE);
    if (bytes_written != BLOCK_SIZE) {
        perror("Failed to write block");
        exit(EXIT_FAILURE);
    }
}

int main() {
    // Open or create the disk file
    int fd = open("disk1", O_CREAT | O_RDWR, 0666);
    if (fd == -1) {
        perror("Failed to create disk file");
        return EXIT_FAILURE;
    }

    char buffer[BLOCK_SIZE] = {0}; // Temporary buffer for writing blocks

    // --- 1. Initialize Superblock ---
    Superblock sb = {
        .total_blocks = TOTAL_BLOCKS,
        .block_size = BLOCK_SIZE,
        .inode_count = MAX_FILES,
        .root_dir_block = BITMAP_BLOCKS + INODE_MAP_BLOCK + INODE_TABLE_BLOCKS
    };
    memcpy(buffer, &sb, sizeof(Superblock));
    write_block(fd, 0, buffer); // Write the superblock to block 0

    // --- 2. Initialize Bitmap ---
    // Mark system blocks as used (superblock, bitmap, inode map, inode table, root directory)
    memset(buffer, 0xFF, BLOCK_SIZE); // Mark all bits as used
    int system_blocks = BITMAP_BLOCKS + INODE_MAP_BLOCK + INODE_TABLE_BLOCKS + ROOT_DIR_BLOCKS;
    for (int i = system_blocks; i < BLOCK_SIZE * 8; i++) {
        buffer[i / 8] &= ~(1 << (i % 8)); // Mark unused blocks as free (set to 0)
    }
    write_block(fd, 1, buffer); // Write the first bitmap block
    memset(buffer, 0, BLOCK_SIZE); // Zero out the next bitmap block
    write_block(fd, 2, buffer);

    // --- 3. Initialize Inode Map ---
    memset(buffer, 0, BLOCK_SIZE);
    buffer[0] = 1; // Mark the root directory inode as used
    write_block(fd, 3, buffer); // Write the inode map to block 3

    // --- 4. Initialize Inode Table ---
    memset(buffer, 0, BLOCK_SIZE);
    for (int i = 4; i < 4 + INODE_TABLE_BLOCKS; i++) {
        write_block(fd, i, buffer); // Zero out inode table blocks
    }

    // --- 5. Initialize Root Directory ---
    DirectoryEntry root_dir[2] = {
        {".", 1},   // Current directory
        {"..", 1}   // Parent directory
    };
    memcpy(buffer, root_dir, sizeof(root_dir));
    write_block(fd, sb.root_dir_block, buffer); // Write root directory entries to disk
    memset(buffer, 0, BLOCK_SIZE);
    write_block(fd, sb.root_dir_block + 1, buffer); // Clear the second root directory block

    printf("Disk Initialized Successfully.\n");
    close(fd);
    return 0;
}
