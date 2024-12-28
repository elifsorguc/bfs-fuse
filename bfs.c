#define FUSE_USE_VERSION 31

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>

#define BLOCK_SIZE 4096
#define MAX_FILES 128
#define FILENAME_LEN 48
#define TOTAL_BLOCKS 4096
#define DIRECT_BLOCKS 8
#define MAX_FILE_SIZE ((DIRECT_BLOCKS + BLOCK_SIZE / sizeof(int)) * BLOCK_SIZE)

// BFS Disk Layout as per Specifications
#define SUPERBLOCK 0
#define BITMAP_BLOCK_START 1
#define BITMAP_BLOCKS 2
#define INODE_MAP_BLOCK 3
#define INODE_TABLE_START 4
#define INODE_TABLE_BLOCKS 8
#define ROOT_DIR_BLOCK_START 12
#define ROOT_DIR_BLOCKS 2
#define DATA_BLOCK_START 14

typedef struct
{
    char name[FILENAME_LEN];
    int inode_num; // Points to the inode for this file
} DirectoryEntry;

typedef struct
{
    int size; // File size in bytes
    int block_pointers[DIRECT_BLOCKS];
    int indirect_pointer; // Pointer to a block containing indirect pointers
    time_t creation_time;
    time_t modification_time;
    mode_t permissions;
    int ref_count; // Reference count for links
} Inode;

// Global Variables
int fd_disk;                                      // Disk file descriptor
unsigned char bitmap[BITMAP_BLOCKS * BLOCK_SIZE]; // Bitmap to manage free/used blocks
unsigned char inode_map[BLOCK_SIZE];              // Inode map (4096 bytes, only first 16 bytes used)
Inode inodes[MAX_FILES];                          // Array of inodes
DirectoryEntry directory[MAX_FILES];              // Array of directory entries

/* Helper Function Prototypes */
void start_timer(struct timespec *start);
double end_timer(struct timespec *start);
int find_file(const char *name);
int find_free_inode();
void initialize_inodes_and_directory();
int read_block(int block_num, void *buf);
int write_block(int block_num, const void *buf);
int find_free_block();
void release_block(int block_num);
void initialize_filesystem();
void save_metadata();
int write_partial_block(int block_num, const void *buf, size_t size);

/* FUSE Operations */
int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi);
int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags);
int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int bfs_unlink(const char *path);
int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int bfs_open(const char *path, struct fuse_file_info *fi);
int bfs_release(const char *path, struct fuse_file_info *fi);
int bfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi);
int bfs_access(const char *path, int mask);
int bfs_rename(const char *from, const char *to, unsigned int flags);

static struct fuse_operations bfs_oper = {
    .getattr = bfs_getattr,
    .readdir = bfs_readdir,
    .create = bfs_create,
    .unlink = bfs_unlink,
    .read = bfs_read,
    .write = bfs_write,
    .open = bfs_open,
    .release = bfs_release,
    .utimens = bfs_utimens,
    .access = bfs_access,
    .rename = bfs_rename,
};

/* Helper Functions */

void start_timer(struct timespec *start) {
    clock_gettime(CLOCK_MONOTONIC, start);
}

double end_timer(struct timespec *start) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - start->tv_sec) * 1e3 + (end.tv_nsec - start->tv_nsec) / 1e6; // Return time in milliseconds
}

/**
 * Finds the index of a file in the directory based on its name.
 * Returns the index if found, -1 otherwise.
 */
int find_file(const char *name)
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, name) == 0)
        {
            return i;
        }
    }
    return -1; // File not found
}

/**
 * Finds a free inode using the inode map.
 * Returns the inode number (1-based) if found, -1 otherwise.
 */
int find_free_inode()
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        if (!(inode_map[byte_idx] & (1 << bit_idx)))
        {
            inode_map[byte_idx] |= (1 << bit_idx); // Mark inode as used
            return i + 1;                          // Inode numbers start at 1
        }
    }
    return -1; // No free inode found
}

/**
 * Releases an inode by clearing its bit in the inode map.
 */
void release_inode(int inode_num)
{
    if (inode_num < 1 || inode_num > MAX_FILES)
        return;
    int idx = inode_num - 1;
    int byte_idx = idx / 8;
    int bit_idx = idx % 8;
    inode_map[byte_idx] &= ~(1 << bit_idx); // Mark inode as free
}

/**
 * Initializes the inodes and directory by loading metadata from disk.
 */
void initialize_inodes_and_directory()
{
    fprintf(stderr, "INITIALIZE: Loading metadata from disk...\n");

    // Load Bitmap (blocks 1-2)
    if (read_block(BITMAP_BLOCK_START, bitmap) != 0)
    {
        fprintf(stderr, "INITIALIZE ERROR: Failed to load bitmap.\n");
        exit(1);
    }

    // Load Inode Map (block 3)
    if (read_block(INODE_MAP_BLOCK, inode_map) != 0)
    {
        fprintf(stderr, "INITIALIZE ERROR: Failed to load inode map.\n");
        exit(1);
    }

    // Load Inodes (blocks 4-11)
    for (int block = 0; block < INODE_TABLE_BLOCKS; block++)
    {
        char block_buf[BLOCK_SIZE];
        if (read_block(INODE_TABLE_START + block, block_buf) != 0)
        {
            fprintf(stderr, "INITIALIZE ERROR: Failed to load inode table block %d.\n", INODE_TABLE_START + block);
            exit(1);
        }
        for (int i = 0; i < BLOCK_SIZE / sizeof(Inode); i++)
        {
            int inode_num = block * (BLOCK_SIZE / sizeof(Inode)) + i;
            if (inode_num < MAX_FILES)
            {
                memcpy(&inodes[inode_num], block_buf + i * sizeof(Inode), sizeof(Inode));
            }
        }
    }

    // Load Root Directory (blocks 12-13)
    int max_dir_entries_per_block = BLOCK_SIZE / sizeof(DirectoryEntry); // 85
    for (int block = 0; block < ROOT_DIR_BLOCKS; block++)
    {
        int start_idx = block * max_dir_entries_per_block;
        for (int i = 0; i < max_dir_entries_per_block && (start_idx + i) < MAX_FILES; i++)
        {
            if (read_block(ROOT_DIR_BLOCK_START + block, &directory[start_idx + i]) != 0)
            {
                fprintf(stderr, "INITIALIZE ERROR: Failed to load directory entry at index %d.\n", start_idx + i);
                exit(1);
            }
        }
    }

    fprintf(stderr, "INITIALIZE: Metadata loaded successfully.\n");
}

/**
 * Reads a block from the disk into the provided buffer.
 * Returns 0 on success, -1 on failure.
 */
int read_block(int block_num, void *buf)
{
    if (lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET) == -1)
        return -1;
    if (read(fd_disk, buf, BLOCK_SIZE) != BLOCK_SIZE)
        return -1;
    return 0;
}

/**
 * Writes a block from the provided buffer to the disk.
 * Returns 0 on success, -1 on failure.
 */
int write_block(int block_num, const void *buf)
{
    if (lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET) == -1)
    {
        perror("WRITE_BLOCK ERROR: lseek failed");
        return -1;
    }

    if (write(fd_disk, buf, BLOCK_SIZE) != BLOCK_SIZE)
    {
        perror("WRITE_BLOCK ERROR: write failed");
        return -1;
    }

    return 0;
}

/**
 * Writes a partial block (useful for writing the inode map which is smaller than a block).
 * Returns 0 on success, -1 on failure.
 */
int write_partial_block(int block_num, const void *buf, size_t size)
{
    if (size > BLOCK_SIZE)
    {
        fprintf(stderr, "WRITE_PARTIAL_BLOCK ERROR: Buffer size exceeds block size\n");
        return -1;
    }

    // Read the existing block to preserve unmodified data
    char existing_block[BLOCK_SIZE];
    if (read_block(block_num, existing_block) != 0)
    {
        perror("WRITE_PARTIAL_BLOCK ERROR: Failed to read existing block");
        return -1;
    }

    // Update the block with new data
    memcpy(existing_block, buf, size);

    // Write the updated block back
    if (write_block(block_num, existing_block) != 0)
    {
        perror("WRITE_PARTIAL_BLOCK ERROR: write failed");
        return -1;
    }

    return 0;
}

/**
 * Finds a free data block using the bitmap.
 * Returns the block number if found, -1 otherwise.
 */
int find_free_block()
{
    for (int i = DATA_BLOCK_START; i < TOTAL_BLOCKS; i++)
    {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        if (!(bitmap[byte_idx] & (1 << bit_idx)))
        {
            bitmap[byte_idx] |= (1 << bit_idx); // Mark block as used
            // Write back the bitmap to the appropriate bitmap block
            int bitmap_block = BITMAP_BLOCK_START + (byte_idx / BLOCK_SIZE);
            if (write_block(bitmap_block, bitmap) != 0)
            {
                fprintf(stderr, "find_free_block ERROR: Failed to update bitmap.\n");
                return -1;
            }
            return i;
        }
    }
    return -1; 
}

/**
 * Releases a data block by clearing its bit in the bitmap.
 */
void release_block(int block_num)
{
    if (block_num < DATA_BLOCK_START || block_num >= TOTAL_BLOCKS)
        return;
    int byte_idx = block_num / 8;
    int bit_idx = block_num % 8;
    bitmap[byte_idx] &= ~(1 << bit_idx); // Mark block as free
    // Write back the bitmap to the appropriate bitmap block
    int bitmap_block = BITMAP_BLOCK_START + (byte_idx / BLOCK_SIZE);
    write_block(bitmap_block, bitmap);
}

/**
 * Initializes the filesystem by loading all necessary metadata.
 */
void initialize_filesystem()
{
    initialize_inodes_and_directory();
    fprintf(stderr, "BFS: Filesystem metadata initialized.\n");
}

/**
 * Saves all metadata (bitmap, inode map, inodes, directory) back to disk.
 */
void save_metadata()
{
    if (write_block(BITMAP_BLOCK_START, bitmap) != 0)
    {
        fprintf(stderr, "SAVE METADATA ERROR: Failed to save bitmap.\n");
    }

    if (write_partial_block(INODE_MAP_BLOCK, inode_map, sizeof(inode_map)) != 0)
    {
        fprintf(stderr, "SAVE METADATA ERROR: Failed to save inode map.\n");
    }

    for (int block = 0; block < INODE_TABLE_BLOCKS; block++)
    {
        char block_buf[BLOCK_SIZE];
        memset(block_buf, 0, BLOCK_SIZE);
        for (int i = 0; i < BLOCK_SIZE / sizeof(Inode); i++)
        {
            int inode_num = block * (BLOCK_SIZE / sizeof(Inode)) + i;
            if (inode_num < MAX_FILES)
            {
                memcpy(block_buf + i * sizeof(Inode), &inodes[inode_num], sizeof(Inode));
            }
        }
        if (write_block(INODE_TABLE_START + block, block_buf) != 0)
        {
            fprintf(stderr, "SAVE METADATA ERROR: Failed to save inode table block %d.\n", INODE_TABLE_START + block);
        }
    }

    fprintf(stderr, "SAVE METADATA: Saving root directory...\n");
    int max_dir_entries_per_block = BLOCK_SIZE / sizeof(DirectoryEntry); // 85
    for (int block = 0; block < ROOT_DIR_BLOCKS; block++)
    {
        int start_idx = block * max_dir_entries_per_block;
        char block_buf[BLOCK_SIZE];
        memset(block_buf, 0, BLOCK_SIZE);
        for (int i = 0; i < max_dir_entries_per_block && (start_idx + i) < MAX_FILES; i++)
        {
            memcpy(block_buf + i * sizeof(DirectoryEntry), &directory[start_idx + i], sizeof(DirectoryEntry));
        }
        if (write_block(ROOT_DIR_BLOCK_START + block, block_buf) != 0)
        {
            fprintf(stderr, "SAVE METADATA ERROR: Failed to save directory block %d.\n", ROOT_DIR_BLOCK_START + block);
        }
    }

    fprintf(stderr, "SAVE METADATA: Metadata saved successfully.\n");
}

/* FUSE Callbacks */

/**
 * Retrieves file attributes.
 */
int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi)
{
    (void)fi;
    fprintf(stderr, "GETATTR: path=%s\n", path);

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0)
    {
        // Root directory
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        fprintf(stderr, "GETATTR: Root directory found\n");
        return 0;
    }

    // Search for the file in the directory
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, path + 1) == 0)
        {
            int inode_idx = directory[i].inode_num - 1; // Convert to 0-based index
            if (inode_idx < 0 || inode_idx >= MAX_FILES)
            {
                fprintf(stderr, "GETATTR ERROR: Invalid inode index=%d for file=%s\n", inode_idx + 1, path);
                return -EIO;
            }

            Inode *inode = &inodes[inode_idx];
            stbuf->st_mode = S_IFREG | inode->permissions;
            stbuf->st_nlink = inode->ref_count;
            stbuf->st_size = inode->size;
            stbuf->st_atime = inode->creation_time;
            stbuf->st_mtime = inode->modification_time;
            stbuf->st_ctime = inode->modification_time;

            fprintf(stderr, "GETATTR: File=%s found, inode=%d\n", path, inode_idx + 1);
            return 0;
        }
    }

    return -ENOENT;
}

/**
 * Opens a file. For simplicity, no specific action is needed here.
 */
int bfs_open(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "OPEN: path=%s\n", path);

    if (find_file(path + 1) == -1)
    {
        fprintf(stderr, "OPEN ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    fprintf(stderr, "OPEN: File=%s opened successfully\n", path);
    return 0; // Success
}

/**
 * Checks file access permissions.
 */
int bfs_access(const char *path, int mask)
{
    fprintf(stderr, "ACCESS: path=%s, mask=%d\n", path, mask);

    if (strcmp(path, "/") == 0)
    {
        // Root directory access
        return 0;
    }

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "ACCESS ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    return 0; // Success
}

/**
 * Reads the contents of a directory.
 */
int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    struct timespec timer;
    start_timer(&timer);
    (void)offset;
    (void)fi;
    (void)flags;

    fprintf(stderr, "READDIR: path=%s\n", path);

    if (strcmp(path, "/") != 0)
    {
        fprintf(stderr, "READDIR ERROR: Only root directory supported\n");
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);

    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0)
        {
            filler(buf, directory[i].name, NULL, 0, 0);
        }
    }

    double elapsed_time = end_timer(&timer);
    fprintf(stderr, "READ: Time taken for readdir: %.2f ms\n", elapsed_time);

    return 0;
}

/**
 * Creates a new file.
 */
int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    struct timespec timer;
    start_timer(&timer);
    (void)fi;
    fprintf(stderr, "CREATE: path=%s, mode=%o\n", path, mode);

    // Check if file already exists
    if (find_file(path + 1) != -1)
    {
        fprintf(stderr, "CREATE ERROR: File=%s already exists\n", path);
        return -EEXIST;
    }

    // Find a free directory entry
    int dir_idx = -1;
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num == 0)
        {
            dir_idx = i;
            break;
        }
    }

    if (dir_idx == -1)
    {
        fprintf(stderr, "CREATE ERROR: Directory full, cannot create file=%s\n", path);
        return -ENOSPC;
    }

    // Find a free inode
    int inode_num = find_free_inode();
    if (inode_num == -1)
    {
        fprintf(stderr, "CREATE ERROR: No free inodes available\n");
        return -ENOSPC;
    }

    // Initialize the inode
    int inode_idx = inode_num - 1;
    Inode *inode = &inodes[inode_idx];
    memset(inode, 0, sizeof(Inode));
    inode->permissions = mode;
    inode->creation_time = inode->modification_time = time(NULL);
    inode->ref_count = 1;

    // Update the directory entry
    strncpy(directory[dir_idx].name, path + 1, FILENAME_LEN - 1);
    directory[dir_idx].name[FILENAME_LEN - 1] = '\0'; // Ensure NULL-termination
    directory[dir_idx].inode_num = inode_num;

    save_metadata();

    double elapsed_time = end_timer(&timer);
    fprintf(stderr, "READ: Time taken for creating: %.2f ms\n", elapsed_time);

    return 0;
}

/**
 * Renames or moves a file from 'from' to 'to'.
 */
int bfs_rename(const char *from, const char *to, unsigned int flags)
{
    fprintf(stderr, "RENAME: from=%s, to=%s, flags=%u\n", from, to, flags);

    if (strcmp(from, "/") == 0 || strcmp(to, "/") == 0)
    {
        fprintf(stderr, "RENAME ERROR: Cannot rename root directory.\n");
        return -EINVAL;
    }

    const char *from_name = from[0] == '/' ? from + 1 : from;
    const char *to_name = to[0] == '/' ? to + 1 : to;

    int from_idx = find_file(from_name);
    if (from_idx == -1)
    {
        fprintf(stderr, "RENAME ERROR: Source file '%s' not found.\n", from_name);
        return -ENOENT;
    }

    // Check if the destination file already exists
    int to_idx = find_file(to_name);
    if (to_idx != -1)
    {
        fprintf(stderr, "RENAME ERROR: Destination file '%s' already exists.\n", to_name);
        return -EEXIST;
    }

    // Find a free directory entry for the destination
    int free_dir_idx = -1;
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num == 0)
        {
            free_dir_idx = i;
            break;
        }
    }

    if (free_dir_idx == -1)
    {
        fprintf(stderr, "RENAME ERROR: No free directory entries available.\n");
        return -ENOSPC;
    }

    // Perform the rename by copying the directory entry to the new name
    directory[free_dir_idx].inode_num = directory[from_idx].inode_num;
    strncpy(directory[free_dir_idx].name, to_name, FILENAME_LEN - 1);
    directory[free_dir_idx].name[FILENAME_LEN - 1] = '\0'; // Ensure NULL-termination

    // Remove the old directory entry
    memset(&directory[from_idx], 0, sizeof(DirectoryEntry));

    // Save metadata to persist changes
    save_metadata();

    fprintf(stderr, "RENAME: Successfully renamed '%s' to '%s'\n", from_name, to_name);
    return 0; // Success
}

/**
 * Deletes a file.
 */
int bfs_unlink(const char *path)
{
    struct timespec timer;
    start_timer(&timer);

    for (int i = 0; i < MAX_FILES; i++)
    {
        if (strcmp(directory[i].name, path + 1) == 0)
        {
            int inode_num = directory[i].inode_num;
            int inode_idx = inode_num - 1; // Adjust to 0-based index

            if (inode_idx < 0 || inode_idx >= MAX_FILES)
            {
                fprintf(stderr, "UNLINK ERROR: Invalid inode number=%d\n", inode_num);
                return -EINVAL;
            }

            Inode *inode = &inodes[inode_idx];

            // Release direct blocks
            for (int j = 0; j < DIRECT_BLOCKS; j++)
            {
                if (inode->block_pointers[j] != 0)
                {
                    release_block(inode->block_pointers[j]);
                    inode->block_pointers[j] = 0;
                }
            }

            // Release indirect blocks if allocated
            if (inode->indirect_pointer != 0)
            {
                char indirect_block[BLOCK_SIZE];
                if (read_block(inode->indirect_pointer, indirect_block) != 0)
                {
                    fprintf(stderr, "UNLINK ERROR: Failed to read indirect block=%d\n", inode->indirect_pointer);
                    return -EIO;
                }

                int *indirect_pointers = (int *)indirect_block;
                for (int j = 0; j < BLOCK_SIZE / sizeof(int); j++)
                {
                    if (indirect_pointers[j] != 0)
                    {
                        release_block(indirect_pointers[j]);
                        indirect_pointers[j] = 0;
                    }
                }

                // Release the indirect pointer block itself
                fprintf(stderr, "UNLINK: Releasing indirect block pointer=%d\n", inode->indirect_pointer);
                release_block(inode->indirect_pointer);
                inode->indirect_pointer = 0;

                // Write back the cleared indirect block
                if (write_block(inode->indirect_pointer, indirect_pointers) != 0)
                {
                    fprintf(stderr, "UNLINK ERROR: Failed to clear indirect block=%d\n", inode->indirect_pointer);
                    return -EIO;
                }
            }

            // Clear directory entry and inode
            fprintf(stderr, "UNLINK: Clearing directory entry for file=%s\n", directory[i].name);
            memset(&directory[i], 0, sizeof(DirectoryEntry));

            memset(inode, 0, sizeof(Inode));
            release_inode(inode_num);

            // Save metadata
            save_metadata();
            fprintf(stderr, "UNLINK: File=%s successfully unlinked\n", path);
            double elapsed_time = end_timer(&timer);
            fprintf(stderr, "READ: Time taken for unlinking : %.2f ms\n", elapsed_time);

            return 0;
        }
    }

    fprintf(stderr, "UNLINK ERROR: File not found at path=%s\n", path);
    
    return -ENOENT;
}

/**
 * Reads data from a file.
 */
int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct timespec timer;
    start_timer(&timer);
    (void)fi;
    fprintf(stderr, "READ: path=%s, size=%zu, offset=%ld\n", path, size, offset);

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "READ ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    int inode_num = directory[file_idx].inode_num;
    int inode_idx = inode_num - 1;
    Inode *inode = &inodes[inode_idx];

    if (offset >= inode->size)
    {
        fprintf(stderr, "READ: Offset beyond EOF for file=%s\n", path);
        return 0;
    }

    if (offset + size > inode->size)
    {
        size = inode->size - offset; // Adjust size to read up to EOF
    }

    size_t bytes_read = 0;
    size_t current_offset = offset;

    while (bytes_read < size)
    {
        size_t block_idx = current_offset / BLOCK_SIZE;
        size_t block_offset = current_offset % BLOCK_SIZE;
        size_t bytes_to_read = BLOCK_SIZE - block_offset;

        if (bytes_to_read > size - bytes_read)
        {
            bytes_to_read = size - bytes_read;
        }

        int actual_block = 0;

        if (block_idx < DIRECT_BLOCKS)
        {
            actual_block = inode->block_pointers[block_idx];
        }
        else
        {
            // Handle indirect blocks
            if (inode->indirect_pointer == 0)
            {
                // Indirect pointer not allocated
                break;
            }

            char indirect_block[BLOCK_SIZE];
            if (read_block(inode->indirect_pointer, indirect_block) != 0)
            {
                fprintf(stderr, "READ ERROR: Failed to read indirect block=%d\n", inode->indirect_pointer);
                return -EIO;
            }

            int *indirect_pointers = (int *)indirect_block;
            size_t indirect_idx = block_idx - DIRECT_BLOCKS;
            if (indirect_idx >= BLOCK_SIZE / sizeof(int))
            {
                // Exceeds maximum file size
                break;
            }

            actual_block = indirect_pointers[indirect_idx];
        }

        if (actual_block == 0)
        {
            // Unallocated block
            memset(buf + bytes_read, 0, bytes_to_read); // Return zeros for unallocated blocks
        }
        else
        {
            char block[BLOCK_SIZE];
            if (read_block(actual_block, block) != 0)
            {
                fprintf(stderr, "READ ERROR: Failed to read block %d for file=%s\n", actual_block, path);
                return -EIO;
            }
            memcpy(buf + bytes_read, block + block_offset, bytes_to_read);
        }

        bytes_read += bytes_to_read;
        current_offset += bytes_to_read;
    }

    fprintf(stderr, "READ: Successfully read %zu bytes from file=%s\n", bytes_read, path);
    double elapsed_time = end_timer(&timer);
    fprintf(stderr, "READ: Time taken for reading %zu bytes: %.2f ms\n", size, elapsed_time);

    return bytes_read;
}

/**
 * Writes data to a file.
 */
int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    struct timespec timer;
    start_timer(&timer);
    (void)fi;
    fprintf(stderr, "WRITE: path=%s, size=%zu, offset=%ld\n", path, size, offset);

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "WRITE ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    int inode_num = directory[file_idx].inode_num;
    int inode_idx = inode_num - 1;
    Inode *inode = &inodes[inode_idx];

    if (offset + size > MAX_FILE_SIZE)
    {
        fprintf(stderr, "WRITE ERROR: File size exceeds maximum allowed size for file=%s\n", path);
        return -EFBIG;
    }

    size_t bytes_written = 0;
    size_t current_offset = offset;

    while (bytes_written < size)
    {
        size_t block_idx = current_offset / BLOCK_SIZE;
        size_t block_offset = current_offset % BLOCK_SIZE;
        size_t bytes_to_write = BLOCK_SIZE - block_offset;

        if (bytes_to_write > size - bytes_written)
        {
            bytes_to_write = size - bytes_written;
        }

        // Allocate block if not already allocated
        if (block_idx < DIRECT_BLOCKS)
        {
            if (inode->block_pointers[block_idx] == 0)
            {
                int free_block = find_free_block();
                if (free_block == -1)
                {
                    fprintf(stderr, "WRITE ERROR: No free blocks available for file=%s\n", path);
                    return -ENOSPC;
                }
                inode->block_pointers[block_idx] = free_block;
            }
        }
        else
        {
            // Handle indirect blocks
            if (inode->indirect_pointer == 0)
            {
                int free_block = find_free_block();
                if (free_block == -1)
                {
                    fprintf(stderr, "WRITE ERROR: No free blocks available for indirect pointers for file=%s\n", path);
                    return -ENOSPC;
                }
                inode->indirect_pointer = free_block;

                // Initialize indirect block
                char indirect_block[BLOCK_SIZE];
                memset(indirect_block, 0, BLOCK_SIZE);
                if (write_block(inode->indirect_pointer, indirect_block) != 0)
                {
                    fprintf(stderr, "WRITE ERROR: Failed to initialize indirect block=%d\n", inode->indirect_pointer);
                    return -EIO;
                }
            }

            // Read indirect block
            char indirect_block[BLOCK_SIZE];
            if (read_block(inode->indirect_pointer, indirect_block) != 0)
            {
                fprintf(stderr, "WRITE ERROR: Failed to read indirect block=%d\n", inode->indirect_pointer);
                return -EIO;
            }

            int *indirect_pointers = (int *)indirect_block;
            size_t indirect_idx = block_idx - DIRECT_BLOCKS;

            if (indirect_idx >= BLOCK_SIZE / sizeof(int))
            {
                fprintf(stderr, "WRITE ERROR: Indirect index out of range for file=%s\n", path);
                return -EFBIG;
            }

            if (indirect_pointers[indirect_idx] == 0)
            {
                int free_block = find_free_block();
                if (free_block == -1)
                {
                    fprintf(stderr, "WRITE ERROR: No free blocks available for file=%s\n", path);
                    return -ENOSPC;
                }
                indirect_pointers[indirect_idx] = free_block;

                // Write back the updated indirect block
                if (write_block(inode->indirect_pointer, indirect_block) != 0)
                {
                    fprintf(stderr, "WRITE ERROR: Failed to update indirect block=%d\n", inode->indirect_pointer);
                    return -EIO;
                }
            }
        }

        // Determine the actual block to write to
        int actual_block = 0;
        if (block_idx < DIRECT_BLOCKS)
        {
            actual_block = inode->block_pointers[block_idx];
        }
        else
        {
            char indirect_block[BLOCK_SIZE];
            if (read_block(inode->indirect_pointer, indirect_block) != 0)
            {
                fprintf(stderr, "WRITE ERROR: Failed to read indirect block=%d\n", inode->indirect_pointer);
                return -EIO;
            }
            int *indirect_pointers = (int *)indirect_block;
            size_t indirect_idx = block_idx - DIRECT_BLOCKS;
            actual_block = indirect_pointers[indirect_idx];
        }

        // Read the existing block data
        char block[BLOCK_SIZE];
        if (read_block(actual_block, block) != 0)
        {
            fprintf(stderr, "WRITE ERROR: Failed to read block=%d for file=%s\n", actual_block, path);
            return -EIO;
        }

        // Write the new data into the block
        memcpy(block + block_offset, buf + bytes_written, bytes_to_write);
        if (write_block(actual_block, block) != 0)
        {
            fprintf(stderr, "WRITE ERROR: Failed to write to block=%d for file=%s\n", actual_block, path);
            return -EIO;
        }

        bytes_written += bytes_to_write;
        current_offset += bytes_to_write;
    }

    // Update file size and modification time
    if (current_offset > inode->size)
    {
        inode->size = current_offset;
    }
    inode->modification_time = time(NULL);

    // Save metadata
    save_metadata();

    double elapsed_time = end_timer(&timer);
    fprintf(stderr, "WRITE: Time taken for writing %zu bytes: %.2f ms\n", size, elapsed_time);

    return bytes_written;
}

/**
 * Releases a file. No specific action needed in this implementation.
 */
int bfs_release(const char *path, struct fuse_file_info *fi)
{
    (void)fi;
    fprintf(stderr, "RELEASE: path=%s\n", path);

    // Nothing to do unless implementing an explicit open file table
    fprintf(stderr, "RELEASE: File=%s closed successfully\n", path);
    return 0;
}

/**
 * Updates file timestamps.
 */
int bfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
    (void)fi;
    fprintf(stderr, "UTIMENS: path=%s\n", path);

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "UTIMENS ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    int inode_num = directory[file_idx].inode_num;
    int inode_idx = inode_num - 1;
    Inode *inode = &inodes[inode_idx];

    inode->creation_time = tv[0].tv_sec;     // Update access time
    inode->modification_time = tv[1].tv_sec; // Update modification time

    // Save metadata
    save_metadata();
    fprintf(stderr, "UTIMENS: Updated timestamps for file=%s\n", path);
    return 0;
}

/* Main Function */

int main(int argc, char *argv[])
{
    fprintf(stderr, "BFS: Starting filesystem...\n");

    // Open the disk file with read-write permissions
    fd_disk = open("disk1", O_RDWR);
    if (fd_disk < 0)
    {
        perror("BFS ERROR: Failed to open disk file");
        return 1;
    }
    fprintf(stderr, "BFS: Disk file 'disk1' opened successfully.\n");

    // Check the size of disk1
    struct stat st;
    if (fstat(fd_disk, &st) != 0)
    {
        perror("BFS ERROR: fstat failed");
        close(fd_disk);
        return 1;
    }
    fprintf(stderr, "Disk size: %ld bytes\n", st.st_size);
    if (st.st_size < TOTAL_BLOCKS * BLOCK_SIZE)
    {
        fprintf(stderr, "BFS ERROR: Disk file size is too small. Expected at least %d bytes.\n", TOTAL_BLOCKS * BLOCK_SIZE);
        close(fd_disk);
        return 1;
    }

    // Initialize the filesystem by loading metadata
    initialize_filesystem();
    fprintf(stderr, "BFS: Filesystem metadata initialized.\n");

    // Mount the filesystem
    fprintf(stderr, "BFS: Mounting filesystem...\n");
    int ret = fuse_main(argc, argv, &bfs_oper, NULL);

    if (ret != 0)
    {
        fprintf(stderr, "BFS ERROR: FUSE failed to initialize or encountered an error.\n");
    }
    else
    {
        fprintf(stderr, "BFS: Filesystem unmounted successfully.\n");
    }

    // Save metadata before exiting
    save_metadata();
    close(fd_disk);
    fprintf(stderr, "BFS: Metadata saved and disk closed.\n");
    return ret;
}
