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

// BFS Disk Layout
#define SUPERBLOCK 0
#define BITMAP_BLOCK 1
#define INODE_TABLE_START 2
#define INODE_TABLE_BLOCKS 4
#define ROOT_DIR_BLOCK 6
#define DATA_BLOCK_START 7

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

int fd_disk;                         // Disk file descriptor
char bitmap[BLOCK_SIZE];             // Bitmap to manage free/used blocks
Inode inodes[MAX_FILES];             // Array of inodes
DirectoryEntry directory[MAX_FILES]; // Array of directory entries
char inode_bitmap[MAX_FILES / 8] = {0};

/* Helper Functions */
int find_file(const char *name);
void initialize_inodes_and_directory();
int read_block(int block_num, void *buf);
int write_block(int block_num, const void *buf);
int find_free_block();
void release_block(int block_num);
void initialize_filesystem();
void save_metadata();
int write_partial_block(int block_num, const void *buf, size_t size);
int find_free_inode();
void release_inode(int inode_num);

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
int bfs_rename(const char *oldpath, const char *newpath);

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

int find_file(const char *name)
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (strcmp(directory[i].name, name) == 0)
        {
            return i;
        }
    }
    return -1; // File not found
}
void initialize_inodes_and_directory()
{
    fprintf(stderr, "INITIALIZE: Loading metadata from disk...\n");

    // Load the bitmap
    if (read_block(BITMAP_BLOCK, bitmap) != 0)
    {
        fprintf(stderr, "INITIALIZE ERROR: Failed to load bitmap.\n");
        exit(1);
    }

    // Verify bitmap size is correct (2 blocks of 4 KB each)
    if (sizeof(bitmap) != BLOCK_SIZE)
    {
        fprintf(stderr, "INITIALIZE ERROR: Bitmap has invalid size.\n");
        exit(1);
    }

    // Load the directory
    if (read_block(ROOT_DIR_BLOCK, directory) != 0)
    {
        fprintf(stderr, "INITIALIZE ERROR: Failed to load directory.\n");
        exit(1); // Exit if directory loading fails
    }

    // Load the inode table
    for (int i = 0; i < MAX_FILES; i++)
    {
        // Check if the inode can be read successfully
        if (read_block(INODE_TABLE_START + i, &inodes[i]) != 0)
        {
            fprintf(stderr, "INITIALIZE ERROR: Failed to load inode %d.\n", i);
            exit(1); // Exit if any inode loading fails
        }
    }

    fprintf(stderr, "INITIALIZE: Metadata loaded successfully.\n");
}

int find_free_inode()
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        if (!(inode_bitmap[byte_idx] & (1 << bit_idx)))
        {
            inode_bitmap[byte_idx] |= (1 << bit_idx);
            return i; // Free inode found
        }
    }
    return -1; // No free inode found
}
void release_inode(int inode_num)
{
    int byte_idx = inode_num / 8;
    int bit_idx = inode_num % 8;
    inode_bitmap[byte_idx] &= ~(1 << bit_idx);
}

int bfs_rename(const char *oldpath, const char *newpath)
{
    // Find the file with the old path
    int file_idx = find_file(oldpath + 1); // Remove the leading '/'
    if (file_idx == -1)
    {
        fprintf(stderr, "RENAME ERROR: File not found: %s\n", oldpath);
        return -ENOENT; // File not found
    }

    // Check if the new file path already exists
    if (find_file(newpath + 1) != -1)
    {
        fprintf(stderr, "RENAME ERROR: File already exists: %s\n", newpath);
        return -EEXIST; // File already exists
    }

    // Rename the file by copying the new path to the directory entry
    strncpy(directory[file_idx].name, newpath + 1, FILENAME_LEN);

    // Save the updated metadata (directory and inodes)
    save_metadata();

    fprintf(stderr, "RENAME: File renamed from %s to %s\n", oldpath, newpath);
    return 0; // Success
}

/* Bitmap Operations */
int find_free_block()
{
    for (int i = DATA_BLOCK_START; i < TOTAL_BLOCKS; i++)
    {
        int byte_idx = i / 8;
        int bit_idx = i % 8;
        if (!(bitmap[byte_idx] & (1 << bit_idx)))
        {
            bitmap[byte_idx] |= (1 << bit_idx);
            write_block(BITMAP_BLOCK, bitmap);
            return i;
        }
    }
    return -1; // No free block found
}

void release_block(int block_num)
{
    int byte_idx = block_num / 8;
    int bit_idx = block_num % 8;
    bitmap[byte_idx] &= ~(1 << bit_idx);
    write_block(BITMAP_BLOCK, bitmap);
}

/* Disk IO */
int read_block(int block_num, void *buf)
{
    if (lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET) == -1)
        return -1;
    if (read(fd_disk, buf, BLOCK_SIZE) != BLOCK_SIZE)
        return -1;
    return 0;
}

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

int write_partial_block(int block_num, const void *buf, size_t size)
{
    if (size > BLOCK_SIZE)
    {
        fprintf(stderr, "WRITE_PARTIAL_BLOCK ERROR: Buffer size exceeds block size\n");
        return -1;
    }

    if (lseek(fd_disk, block_num * BLOCK_SIZE, SEEK_SET) == -1)
    {
        perror("WRITE_PARTIAL_BLOCK ERROR: lseek failed");
        return -1;
    }

    if (write(fd_disk, buf, size) != size)
    {
        perror("WRITE_PARTIAL_BLOCK ERROR: write failed");
        return -1;
    }

    return 0;
}

/* Initialization */
void initialize_filesystem()
{
    // Verify bitmap size
    if (sizeof(bitmap) != BLOCK_SIZE)
    {
        fprintf(stderr, "ERROR: Invalid bitmap size.\n");
        exit(1);
    }

    // Load bitmap
    read_block(BITMAP_BLOCK, bitmap);

    // Load directory
    read_block(ROOT_DIR_BLOCK, directory);

    // Load inodes, checking for overflows
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (INODE_TABLE_START + i >= TOTAL_BLOCKS)
        {
            fprintf(stderr, "ERROR: Inode index exceeds available blocks.\n");
            exit(1);
        }
        read_block(INODE_TABLE_START + i, &inodes[i]);
    }
}

void save_metadata() {
    fprintf(stderr, "SAVE METADATA: Saving inode bitmap...\n");
    if (write_partial_block(BITMAP_BLOCK, inode_bitmap, sizeof(inode_bitmap)) != 0) {
        fprintf(stderr, "SAVE METADATA ERROR: Failed to save inode bitmap.\n");
    }

    fprintf(stderr, "SAVE METADATA: Saving block bitmap...\n");
    if (write_partial_block(BITMAP_BLOCK + 1, bitmap, BLOCK_SIZE) != 0) {
        fprintf(stderr, "SAVE METADATA ERROR: Failed to save block bitmap.\n");
    }

    fprintf(stderr, "SAVE METADATA: Saving directory and inode table...\n");
    if (write_block(ROOT_DIR_BLOCK, directory) != 0) {
        fprintf(stderr, "SAVE METADATA ERROR: Failed to save directory.\n");
    }

    for (int i = 0; i < MAX_FILES; i++) {
        if (write_block(INODE_TABLE_START + i, &inodes[i]) != 0) {
            fprintf(stderr, "SAVE METADATA ERROR: Failed to save inode %d.\n", i);
        }
    }
    fprintf(stderr, "SAVE METADATA: Metadata saved successfully.\n");
}


/* FUSE Callbacks */
int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    fprintf(stderr, "GETATTR: path=%s\n", path);

    memset(stbuf, 0, sizeof(struct stat));
    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755; // Root directory
        stbuf->st_nlink = 2;
        fprintf(stderr, "GETATTR: Root directory found\n");
        return 0;
    }

    int file_idx = find_file(path + 1); // Remove leading '/'
    if (file_idx == -1) {
        fprintf(stderr, "GETATTR ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    DirectoryEntry *entry = &directory[file_idx];
    Inode *inode = &inodes[entry->inode_num - 1];

    stbuf->st_mode = S_IFREG | inode->permissions;
    stbuf->st_nlink = inode->ref_count;
    stbuf->st_size = inode->size;
    stbuf->st_atime = inode->creation_time;
    stbuf->st_mtime = inode->modification_time;
    stbuf->st_ctime = inode->modification_time;

    fprintf(stderr, "GETATTR: File=%s found, inode=%d\n", path, entry->inode_num);
    return 0;
}


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

int bfs_access(const char *path, int mask)
{
    fprintf(stderr, "ACCESS: path=%s, mask=%d\n", path, mask);

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "ACCESS ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    // Simplified access check
    fprintf(stderr, "ACCESS: File=%s is accessible\n", path);
    return 0; // Success
}

int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    fprintf(stderr, "READDIR: path=%s\n", path);

    if (strcmp(path, "/") != 0)
    {
        fprintf(stderr, "READDIR ERROR: Only root directory supported\n");
        return -ENOENT;
    }

    // Add current directory and parent directory entries
    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);

    // Add entries for files in the root directory
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0)
        {
            fprintf(stderr, "READDIR: Found entry=%s\n", directory[i].name);
            filler(buf, directory[i].name, NULL, 0, 0);
        }
    }

    fprintf(stderr, "READDIR: Completed listing directory contents\n");
    return 0;
}

int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    fprintf(stderr, "CREATE: path=%s, mode=%o\n", path, mode);

    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num == 0)
        {
            // Check for duplicate name
            if (find_file(path + 1) != -1)
            {
                fprintf(stderr, "CREATE ERROR: File=%s already exists\n", path);
                return -EEXIST;
            }

            // Assign a free inode
            int inode_idx = find_free_inode();
            if (inode_idx == -1)
            {
                fprintf(stderr, "CREATE ERROR: No free inodes available\n");
                return -ENOSPC;
            }

            strncpy(directory[i].name, path + 1, FILENAME_LEN);
            directory[i].inode_num = inode_idx + 1; // 1-based indexing

            Inode *inode = &inodes[inode_idx];
            memset(inode, 0, sizeof(Inode));
            inode->permissions = mode;
            inode->creation_time = inode->modification_time = time(NULL);
            inode->ref_count = 1;

            save_metadata();
            fprintf(stderr, "CREATE: File=%s created successfully\n", path);
            return 0;
        }
    }

    fprintf(stderr, "CREATE ERROR: Directory full, cannot create file=%s\n", path);
    return -ENOSPC;
}


int bfs_unlink(const char *path)
{
    fprintf(stderr, "UNLINK: Attempting to delete file at path=%s\n", path);

    for (int i = 0; i < MAX_FILES; i++)
    {
        if (strcmp(directory[i].name, path + 1) == 0)
        {
            int inode_num = directory[i].inode_num - 1; // Convert to 0-based index
            release_inode(inode_num);

            Inode *inode = &inodes[inode_num];
            for (int j = 0; j < DIRECT_BLOCKS; j++)
            {
                if (inode->block_pointers[j] != 0)
                {
                    release_block(inode->block_pointers[j]);
                }
            }

            memset(&directory[i], 0, sizeof(DirectoryEntry));
            memset(inode, 0, sizeof(Inode));

            save_metadata();
            fprintf(stderr, "UNLINK: File=%s successfully unlinked\n", path);
            return 0;
        }
    }

    fprintf(stderr, "UNLINK ERROR: File not found at path=%s\n", path);
    return -ENOENT;
}


int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "READ: path=%s, size=%zu, offset=%ld\n", path, size, offset);

    int file_idx = find_file(path + 1);
    if (file_idx == -1) {
        fprintf(stderr, "READ ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    Inode *inode = &inodes[directory[file_idx].inode_num - 1];
    if (offset >= inode->size) {
        fprintf(stderr, "READ: Offset beyond EOF for file=%s\n", path);
        return 0; // EOF
    }

    size_t bytes_read = 0;
    while (bytes_read < size && offset + bytes_read < inode->size) {
        size_t block_idx = (offset + bytes_read) / BLOCK_SIZE;
        size_t block_offset = (offset + bytes_read) % BLOCK_SIZE;

        if (block_idx >= DIRECT_BLOCKS) {
            fprintf(stderr, "READ ERROR: Indirect blocks not implemented for file=%s\n", path);
            return -EIO;
        }

        char block[BLOCK_SIZE] = {0};

        if (inode->block_pointers[block_idx] == 0) {
            fprintf(stderr, "READ ERROR: Unallocated block %zu for file=%s\n", block_idx, path);
            break;
        }

        if (read_block(inode->block_pointers[block_idx], block) != 0) {
            fprintf(stderr, "READ ERROR: Failed to read block %zu for file=%s\n", block_idx, path);
            return -EIO;
        }

        size_t bytes_to_copy = BLOCK_SIZE - block_offset;
        if (bytes_to_copy > size - bytes_read) {
            bytes_to_copy = size - bytes_read;
        }
        if (bytes_to_copy > inode->size - offset - bytes_read) {
            bytes_to_copy = inode->size - offset - bytes_read;
        }

        memcpy(buf + bytes_read, block + block_offset, bytes_to_copy);
        bytes_read += bytes_to_copy;
    }

    fprintf(stderr, "READ: Successfully read %zu bytes from file=%s\n", bytes_read, path);
    return bytes_read;
}


int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "WRITE: path=%s, size=%zu, offset=%ld\n", path, size, offset);

    int file_idx = find_file(path + 1);
    if (file_idx == -1) {
        fprintf(stderr, "WRITE ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    Inode *inode = &inodes[directory[file_idx].inode_num - 1];
    if (offset + size > MAX_FILE_SIZE) {
        fprintf(stderr, "WRITE ERROR: File size exceeds maximum for file=%s\n", path);
        return -EFBIG;
    }

    size_t bytes_written = 0;
    while (bytes_written < size) {
        size_t block_idx = (offset + bytes_written) / BLOCK_SIZE;
        size_t block_offset = (offset + bytes_written) % BLOCK_SIZE;

        if (block_idx >= DIRECT_BLOCKS) {
            fprintf(stderr, "WRITE ERROR: Indirect blocks not implemented for file=%s\n", path);
            return -EIO;
        }

        char block[BLOCK_SIZE] = {0};
        if (inode->block_pointers[block_idx] == 0) {
            int block_num = find_free_block();
            if (block_num == -1) {
                fprintf(stderr, "WRITE ERROR: No free blocks for file=%s\n", path);
                return -ENOSPC;
            }
            inode->block_pointers[block_idx] = block_num;
            fprintf(stderr, "WRITE: Allocated new block %d for file=%s\n", block_num, path);
        } else {
            if (read_block(inode->block_pointers[block_idx], block) != 0) {
                fprintf(stderr, "WRITE ERROR: Failed to read block %zu for file=%s\n", block_idx, path);
                return -EIO;
            }
        }

        size_t bytes_to_write = BLOCK_SIZE - block_offset;
        if (bytes_to_write > size - bytes_written) {
            bytes_to_write = size - bytes_written;
        }

        memcpy(block + block_offset, buf + bytes_written, bytes_to_write);
        if (write_block(inode->block_pointers[block_idx], block) != 0) {
            fprintf(stderr, "WRITE ERROR: Failed to write block %zu for file=%s\n", block_idx, path);
            return -EIO;
        }

        bytes_written += bytes_to_write;
    }

    // Update size and save metadata
    if (offset + bytes_written > inode->size) {
        inode->size = offset + bytes_written;
    }
    inode->modification_time = time(NULL);

    save_metadata();
    fprintf(stderr, "WRITE: Successfully wrote %zu bytes to file=%s\n", bytes_written, path);
    return bytes_written;
}



int bfs_release(const char *path, struct fuse_file_info *fi)
{
    fprintf(stderr, "RELEASE: path=%s\n", path);

    // Nothing to do unless we implement an explicit open file table
    fprintf(stderr, "RELEASE: File=%s closed successfully\n", path);
    return 0;
}

int bfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi)
{
    fprintf(stderr, "UTIMENS: path=%s\n", path);

    int file_idx = find_file(path + 1);
    if (file_idx == -1)
    {
        fprintf(stderr, "UTIMENS ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    Inode *inode = &inodes[directory[file_idx].inode_num - 1];
    inode->creation_time = tv[0].tv_sec;     // Update access time
    inode->modification_time = tv[1].tv_sec; // Update modification time

    save_metadata();
    fprintf(stderr, "UTIMENS: Updated timestamps for file=%s\n", path);
    return 0;
}

int main(int argc, char *argv[])
{
    fprintf(stderr, "BFS: Starting filesystem...\n");

    fd_disk = open("disk1", O_RDWR);
    if (fd_disk < 0)
    {
        perror("BFS ERROR: Failed to open disk file");
        return 1;
    }
    fprintf(stderr, "BFS: Disk file 'disk1' opened successfully.\n");

    initialize_inodes_and_directory();
    fprintf(stderr, "BFS: Filesystem metadata initialized.\n");

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

    save_metadata();
    close(fd_disk);
    fprintf(stderr, "BFS: Metadata saved and disk closed.\n");
    return ret;
}
