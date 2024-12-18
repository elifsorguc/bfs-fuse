/*
    The bfs program, hence the file system, will run in user-space.
    It will store file and meta-data in a regular Linux file.
    This Linux file will be the disk of the file system.
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

#define BLOCK_SIZE 4096
#define MAX_FILES 128
#define FILENAME_LEN 48
#define TOTAL_BLOCKS 4096
#define DEBUG 1

// Debugging macro
#define DEBUG_PRINT(fmt, ...)                                                                      \
    do                                                                                             \
    {                                                                                              \
        if (DEBUG)                                                                                 \
            fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, __LINE__, __func__, __VA_ARGS__); \
    } while (0)

// Function Prototypes
int find_file(const char *name);
static int bfs_rename(const char *from, const char *to, unsigned int flags);
static int bfs_open(const char *path, struct fuse_file_info *fi);
static int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
static int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

int read_block(int fd_disk, void *block, int k);
int write_block(int fd_disk, void *block, int k);
int find_free_block();
void update_bitmap(int block_num, int value);

// Data Structures
typedef struct
{
    char name[FILENAME_LEN];
    int inode_num;
} DirectoryEntry;

typedef struct
{
    int size;
    int block_pointers[8]; // Direct pointers
    int indirect_pointer;  // Indirect pointer
} Inode;

int fd_disk;
DirectoryEntry directory[MAX_FILES];
Inode inodes[MAX_FILES];

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

// Find the index of the first free block
int find_free_block()
{
    char bitmap[BLOCK_SIZE];
    if (read_block(fd_disk, bitmap, 1) != 0)
    {
        printf("Error reading bitmap\n");
        return -1;
    }

    for (int i = 0; i < TOTAL_BLOCKS; i++)
    {
        int byte = i / 8;
        int bit = i % 8;
        if (!(bitmap[byte] & (1 << bit)))
        {
            bitmap[byte] |= (1 << bit);
            if (write_block(fd_disk, bitmap, 1) != 0)
            {
                printf("Error updating bitmap\n");
                return -1;
            }
            return i;
        }
    }
    return -1; // No free block found
}

// Update the bitmap to mark a block as allocated or free
void update_bitmap(int block_num, int value)
{
    char bitmap[BLOCK_SIZE];
    if (read_block(fd_disk, bitmap, 1) != 0)
    {
        printf("Error reading bitmap\n");
        return;
    }

    int byte = block_num / 8;
    int bit = block_num % 8;

    if (value)
        bitmap[byte] |= (1 << bit); // Mark as allocated
    else
        bitmap[byte] &= ~(1 << bit); // Mark as free

    if (write_block(fd_disk, bitmap, 1) != 0)
    {
        printf("Error updating bitmap\n");
    }
}

// FUSE Operations
static int bfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
    (void)fi; // Unused parameter
    int idx = find_file(path + 1);
    if (idx == -1)
        return -ENOENT;

    // Simulate updating timestamps (no persistent effect)
    printf("Updating timestamps for file: %s\n", path);
    return 0; // Success
}

int find_file(const char *name)
{
    DEBUG_PRINT("Searching for file: %s\n", name);
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, name) == 0)
        {
            DEBUG_PRINT("Found file %s at index %d\n", name, i);
            return i;
        }
    }
    DEBUG_PRINT("File %s not found\n", name);
    return -1;
}

static int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi)
{
    (void)fi;
    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0)
    {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }
    int idx = find_file(path + 1);
    if (idx != -1)
    {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = inodes[directory[idx].inode_num - 1].size;
        return 0;
    }
    return -ENOENT;
}

static int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                       struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    DEBUG_PRINT("Listing directory: %s\n", path);
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0)
        {
            DEBUG_PRINT("Found file: %s (Index: %d)\n", directory[i].name, i);
            filler(buf, directory[i].name, NULL, 0, 0);
        }
    }
    return 0;
}

static int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    DEBUG_PRINT("Creating file: %s\n", path);
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num == 0)
        {
            strncpy(directory[i].name, path + 1, FILENAME_LEN);
            directory[i].inode_num = i + 1;

            memset(&inodes[i], 0, sizeof(Inode)); // Initialize inode
            return 0;
        }
    }
    DEBUG_PRINT("Failed to create file: %s (No space)\n", path);
    return -ENOSPC;
}

static int bfs_unlink(const char *path)
{
    DEBUG_PRINT("Deleting file: %s\n", path);
    int idx = find_file(path + 1);
    if (idx == -1)
        return -ENOENT;

    int inode_num = directory[idx].inode_num;
    if (inode_num > 0)
    {
        Inode *inode = &inodes[inode_num - 1];
        for (int i = 0; i < 8; i++)
        {
            if (inode->block_pointers[i] > 0)
                update_bitmap(inode->block_pointers[i], 0);
        }

        memset(inode, 0, sizeof(Inode));
        directory[idx].inode_num = 0;
        return 0;
    }
    return -ENOENT;
}

static int bfs_rename(const char *from, const char *to, unsigned int flags)
{
    (void)flags;
    int idx = find_file(from + 1);
    if (idx != -1)
    {
        strncpy(directory[idx].name, to + 1, FILENAME_LEN);
        return 0;
    }
    return -ENOENT;
}

static int bfs_open(const char *path, struct fuse_file_info *fi)
{
    (void)fi;
    int idx = find_file(path + 1);
    return (idx != -1) ? 0 : -ENOENT;
}

static int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    (void)fi;
    int idx = find_file(path + 1);
    if (idx == -1)
        return -ENOENT;

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    if (offset >= inode->size)
        return 0;

    size_t bytes_read = 0;
    size_t block_index = offset / BLOCK_SIZE;
    size_t block_offset = offset % BLOCK_SIZE;

    while (bytes_read < size && block_index < 8 && inode->block_pointers[block_index] > 0)
    {
        char block[BLOCK_SIZE];
        read_block(fd_disk, block, inode->block_pointers[block_index]);

        size_t bytes_to_read = BLOCK_SIZE - block_offset;
        if (bytes_to_read > size - bytes_read)
            bytes_to_read = size - bytes_read;

        memcpy(buf + bytes_read, block + block_offset, bytes_to_read);
        bytes_read += bytes_to_read;
        block_index++;
        block_offset = 0;
    }

    return bytes_read;
}

static int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    (void)fi;
    int idx = find_file(path + 1);
    if (idx == -1)
        return -ENOENT;

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    size_t bytes_written = 0;
    size_t block_index = offset / BLOCK_SIZE;
    size_t block_offset = offset % BLOCK_SIZE;

    while (bytes_written < size)
    {
        if (block_index >= 8)
            return -EFBIG;

        if (inode->block_pointers[block_index] == 0)
        {
            int free_block = find_free_block();
            if (free_block == -1)
                return -ENOSPC;

            inode->block_pointers[block_index] = free_block;
            update_bitmap(free_block, 1);
        }

        char block[BLOCK_SIZE];
        read_block(fd_disk, block, inode->block_pointers[block_index]);

        size_t bytes_to_write = BLOCK_SIZE - block_offset;
        if (bytes_to_write > size - bytes_written)
            bytes_to_write = size - bytes_written;

        memcpy(block + block_offset, buf + bytes_written, bytes_to_write);
        write_block(fd_disk, block, inode->block_pointers[block_index]);

        bytes_written += bytes_to_write;
        block_index++;
        block_offset = 0;
    }

    if (offset + bytes_written > inode->size)
        inode->size = offset + bytes_written;

    return bytes_written;
}

static struct fuse_operations bfs_oper = {
    .getattr = bfs_getattr,
    .readdir = bfs_readdir,
    .create = bfs_create,
    .unlink = bfs_unlink,
    .rename = bfs_rename,
    .open = bfs_open,
    .read = bfs_read,
    .write = bfs_write,
    .utimens = bfs_utimens,
};

int main(int argc, char *argv[])
{
    fd_disk = open("disk1", O_RDWR);
    if (fd_disk < 0)
    {
        perror("Failed to open disk file");
        return 1;
    }

    int ret = fuse_main(argc, argv, &bfs_oper, NULL);
    close(fd_disk);
    return ret;
}
