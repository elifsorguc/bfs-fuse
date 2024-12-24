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

// Superblock structure
typedef struct {
    int total_blocks;         // Total number of blocks
    int block_size;           // Block size in bytes
    int inode_count;          // Total number of inodes
    int root_dir_block;       // Start block of the root directory
} Superblock;

// Directory Entry structure
typedef struct {
    char name[FILENAME_LEN];  // File name
    int inode_num;            // Inode number
} DirectoryEntry;

// Utility Functions
int write_block(int fd, void *data, int block_num) {
    lseek(fd, block_num * BLOCK_SIZE, SEEK_SET);
    ssize_t bytes_written = write(fd, data, BLOCK_SIZE);
    if (bytes_written != BLOCK_SIZE) {
        perror("Write failed");
        return -1;
    }
    return 0;
}

int main() {
    int fd = open("disk1", O_CREAT | O_RDWR, 0666);
    if (fd < 0) {
        perror("Failed to create disk file");
        return 1;
    }

    char buffer[BLOCK_SIZE] = {0};

    // 1. Initialize the Superblock
    Superblock sb = {TOTAL_BLOCKS, BLOCK_SIZE, MAX_FILES, BITMAP_BLOCKS + INODE_MAP_BLOCK + 1};
    memcpy(buffer, &sb, sizeof(Superblock));
    if (write_block(fd, buffer, 0) != 0) {
        close(fd);
        return 1;
    }
    printf("Superblock initialized.\n");

    // 2. Initialize the Bitmap
    memset(buffer, 0xFF, BLOCK_SIZE); // Mark all system blocks as used
    buffer[0] = 0x03; // First three blocks (superblock + bitmap blocks) are used
    if (write_block(fd, buffer, 1) != 0 || write_block(fd, buffer, 2) != 0) {
        close(fd);
        return 1;
    }
    printf("Bitmap initialized.\n");

    // 3. Initialize the Inode Map
    memset(buffer, 0, BLOCK_SIZE);
    buffer[0] = 1; // Mark the root directory inode as used
    if (write_block(fd, buffer, 3) != 0) {
        close(fd);
        return 1;
    }
    printf("Inode map initialized.\n");

    // 4. Initialize the Inode Table
    for (int i = 4; i < 4 + INODE_TABLE_BLOCKS; i++) {
        memset(buffer, 0, BLOCK_SIZE);
        if (write_block(fd, buffer, i) != 0) {
            close(fd);
            return 1;
        }
    }
    printf("Inode table initialized.\n");

    // 5. Initialize the Root Directory
    DirectoryEntry root_dir[2] = { {".", 1}, {"..", 1} };
    memset(buffer, 0, BLOCK_SIZE);
    memcpy(buffer, root_dir, sizeof(root_dir));
    if (write_block(fd, buffer, sb.root_dir_block) != 0) {
        close(fd);
        return 1;
    }
    printf("Root directory initialized.\n");

    // 6. Clear all remaining blocks
    memset(buffer, 0, BLOCK_SIZE);
    for (int i = sb.root_dir_block + ROOT_DIR_BLOCKS; i < TOTAL_BLOCKS; i++) {
        if (write_block(fd, buffer, i) != 0) {
            close(fd);
            return 1;
        }
    }
    printf("Disk blocks cleared.\n");

    printf("Disk initialized successfully.\n");
    close(fd);
    return 0;
}
