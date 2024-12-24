/*
 * BFS (Basic File System) implementation using FUSE.
 * This filesystem operates in user-space and simulates a disk using a Linux file.
 */

#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <stdlib.h>

#define BLOCK_SIZE 4096       // Block size in bytes (4 KB)
#define MAX_FILES 128         // Maximum number of files
#define FILENAME_LEN 48       // Maximum filename length (including null terminator)
#define DIRECT_BLOCKS 8       // Number of direct block pointers per inode
#define MAX_FILE_SIZE (DIRECT_BLOCKS + 1024) * BLOCK_SIZE // Maximum file size

typedef struct {
    char name[FILENAME_LEN];  // Name of the file
    int inode_num;            // Inode number for the file
} DirectoryEntry;

typedef struct {
    int size;                 // Size of the file in bytes
    int block_pointers[DIRECT_BLOCKS];    // Direct block pointers
    int indirect_pointer;     // Pointer to an indirect block
} Inode;

typedef struct {
    int total_blocks;         // Total number of blocks on the disk
    int block_size;           // Block size (4 KB)
    int inode_count;          // Total number of inodes
    int root_dir_block;       // Start block of the root directory
} Superblock;

// Global variables
int fd_disk;                  // File descriptor for the disk file
DirectoryEntry directory[MAX_FILES]; // Root directory entries
Inode inodes[MAX_FILES];      // Array of inodes
char bitmap[BLOCK_SIZE * 2];  // Bitmap for free space
Superblock superblock;        // Superblock for filesystem metadata

// Utility functions for block management
int read_block(int block_num, void *buffer) {
    lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET);
    return read(fd_disk, buffer, BLOCK_SIZE);
}

int write_block(int block_num, void *buffer) {
    lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET);
    return write(fd_disk, buffer, BLOCK_SIZE);
}

// Find the next free block in the bitmap
int find_free_block() {
    for (int i = 0; i < BLOCK_SIZE * 2 * 8; i++) {
        if (!(bitmap[i / 8] & (1 << (i % 8)))) {
            return i;
        }
    }
    return -1; // No free block
}

// Mark a block as used in the bitmap
void mark_block_used(int block_num) {
    bitmap[block_num / 8] |= (1 << (block_num % 8));
}

// Mark a block as free in the bitmap
void mark_block_free(int block_num) {
    bitmap[block_num / 8] &= ~(1 << (block_num % 8));
}

// Utility to find a directory entry by file name
int find_file(const char *name) {
    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, name) == 0) {
            return i;
        }
    }
    return -1;
}

// Function to get attributes of a file or directory
static int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void)fi;
    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) {
        // Handle root directory
        stbuf->st_mode = S_IFDIR | 0755; // Directory with rwxr-xr-x permissions
        stbuf->st_nlink = 2;             // Link count for directories
        return 0;
    }

    int idx = find_file(path + 1); // Skip the initial '/'
    if (idx != -1) {
        // File found, populate attributes
        stbuf->st_mode = S_IFREG | 0644; // Regular file with rw-r--r-- permissions
        stbuf->st_nlink = 1;             // Link count for files
        stbuf->st_size = inodes[directory[idx].inode_num - 1].size; // File size
        return 0;
    }

    return -ENOENT; // File not found
}

// Function to list files in a directory
static int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                       struct fuse_file_info *fi, enum fuse_readdir_flags flags) {
    (void)offset;
    (void)fi;
    (void)flags;

    if (strcmp(path, "/") != 0) {
        return -ENOENT; // Only the root directory exists
    }

    // Add special entries for the current and parent directory
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);

    // Add each file in the directory
    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num > 0) {
            filler(buf, directory[i].name, NULL, 0, 0);
        }
    }
    return 0;
}

// Function to create a file
static int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num == 0) { // Find an empty directory slot
            // Create a new file
            strncpy(directory[i].name, path + 1, FILENAME_LEN); // Skip initial '/'
            directory[i].inode_num = i + 1;                    // Assign an inode
            inodes[i].size = 0;                                // Initialize file size
            memset(inodes[i].block_pointers, 0, sizeof(inodes[i].block_pointers)); // Clear block pointers
            inodes[i].indirect_pointer = 0;                    // No indirect block
            return 0;
        }
    }
    return -ENOSPC; // No space in directory
}

// Function to delete a file
static int bfs_unlink(const char *path) {
    int idx = find_file(path + 1);
    if (idx != -1) {
        int inode_num = directory[idx].inode_num;
        if (inode_num > 0) {
            // Free all allocated blocks
            for (int i = 0; i < DIRECT_BLOCKS; i++) {
                if (inodes[inode_num - 1].block_pointers[i] > 0) {
                    mark_block_free(inodes[inode_num - 1].block_pointers[i]);
                }
            }

            // Free indirect blocks if used
            if (inodes[inode_num - 1].indirect_pointer > 0) {
                char indirect_block[BLOCK_SIZE];
                read_block(inodes[inode_num - 1].indirect_pointer, indirect_block);
                for (int i = 0; i < BLOCK_SIZE / 4; i++) {
                    int *indirect_pointers = (int *)indirect_block;
                    if (indirect_pointers[i] > 0) {
                        mark_block_free(indirect_pointers[i]);
                    }
                }
                mark_block_free(inodes[inode_num - 1].indirect_pointer);
            }

            memset(&inodes[inode_num - 1], 0, sizeof(Inode)); // Clear the inode
            directory[idx].inode_num = 0;                    // Mark directory entry as unused
            return 0;
        }
    }
    return -ENOENT; // File not found
}

// Function to rename a file
static int bfs_rename(const char *from, const char *to, unsigned int flags) {
    (void)flags;
    int idx = find_file(from + 1);
    if (idx != -1) {
        strncpy(directory[idx].name, to + 1, FILENAME_LEN); // Update file name
        return 0;
    }
    return -ENOENT; // File not found
}

// Function to open a file (stub, no special handling needed)
static int bfs_open(const char *path, struct fuse_file_info *fi) {
    return 0; // Always succeeds
}

// Function to read data from a file
static int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) {
        return -ENOENT; // File not found
    }

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    if (offset >= inode->size) {
        return 0; // Trying to read beyond EOF
    }

    size = (offset + size > inode->size) ? (inode->size - offset) : size; // Clamp size
    size_t bytes_read = 0;
    char block[BLOCK_SIZE];

    while (size > 0) {
        int block_idx = offset / BLOCK_SIZE;
        int block_offset = offset % BLOCK_SIZE;

        // Determine which block to read
        int block_num = (block_idx < DIRECT_BLOCKS) ? inode->block_pointers[block_idx] : -1;
        if (block_idx >= DIRECT_BLOCKS) {
            if (inode->indirect_pointer > 0) {
                char indirect_block[BLOCK_SIZE];
                read_block(inode->indirect_pointer, indirect_block);
                int *indirect_pointers = (int *)indirect_block;
                block_num = indirect_pointers[block_idx - DIRECT_BLOCKS];
            }
        }

        if (block_num <= 0) {
            break; // Block not allocated
        }

        read_block(block_num, block);
        size_t bytes_to_copy = (BLOCK_SIZE - block_offset > size) ? size : (BLOCK_SIZE - block_offset);
        memcpy(buf + bytes_read, block + block_offset, bytes_to_copy);

        size -= bytes_to_copy;
        offset += bytes_to_copy;
        bytes_read += bytes_to_copy;
    }

    return bytes_read;
}

// Function to write data to a file
static int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) {
        return -ENOENT; // File not found
    }

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    size_t bytes_written = 0;
    char block[BLOCK_SIZE];

    while (size > 0) {
        int block_idx = offset / BLOCK_SIZE;
        int block_offset = offset % BLOCK_SIZE;

        // Determine which block to write
        int block_num = (block_idx < DIRECT_BLOCKS) ? inode->block_pointers[block_idx] : -1;
        if (block_idx >= DIRECT_BLOCKS) {
            if (inode->indirect_pointer == 0) {
                int new_block = find_free_block();
                if (new_block == -1) {
                    return -ENOSPC; // No space for indirect block
                }
                inode->indirect_pointer = new_block;
                char indirect_block[BLOCK_SIZE] = {0};
                write_block(new_block, indirect_block);
            }

            char indirect_block[BLOCK_SIZE];
            read_block(inode->indirect_pointer, indirect_block);
            int *indirect_pointers = (int *)indirect_block;

            if (indirect_pointers[block_idx - DIRECT_BLOCKS] == 0) {
                int new_block = find_free_block();
                if (new_block == -1) {
                    return -ENOSPC; // No space for new block
                }
                indirect_pointers[block_idx - DIRECT_BLOCKS] = new_block;
                write_block(inode->indirect_pointer, indirect_block);
            }

            block_num = indirect_pointers[block_idx - DIRECT_BLOCKS];
        } else {
            if (block_num == 0) {
                int new_block = find_free_block();
                if (new_block == -1) {
                    return -ENOSPC; // No space for new block
                }
                inode->block_pointers[block_idx] = new_block;
                block_num = new_block;
            }
        }

        read_block(block_num, block);
        size_t bytes_to_copy = (BLOCK_SIZE - block_offset > size) ? size : (BLOCK_SIZE - block_offset);
        memcpy(block + block_offset, buf + bytes_written, bytes_to_copy);
        write_block(block_num, block);

        size -= bytes_to_copy;
        offset += bytes_to_copy;
        bytes_written += bytes_to_copy;
    }

    // Update file size if necessary
    if (offset > inode->size) {
        inode->size = offset;
    }

    return bytes_written;
}

// FUSE operations structure linking our functions to FUSE
static struct fuse_operations bfs_oper = {
    .getattr = bfs_getattr,
    .readdir = bfs_readdir,
    .create = bfs_create,
    .unlink = bfs_unlink,
    .rename = bfs_rename,
    .open = bfs_open,
    .read = bfs_read,
    .write = bfs_write,
};

// Main function to start the FUSE filesystem
int main(int argc, char *argv[]) {
    // Open the simulated disk file
    fd_disk = open("disk1", O_RDWR);
    if (fd_disk == -1) {
        perror("Failed to open disk");
        return 1;
    }

    // Load the bitmap and superblock from disk
    read_block(1, bitmap); // Load the first bitmap block
    read_block(2, bitmap + BLOCK_SIZE); // Load the second bitmap block
    read_block(0, &superblock); // Load the superblock

    // Mount the filesystem and start FUSE
    return fuse_main(argc, argv, &bfs_oper, NULL);
}
