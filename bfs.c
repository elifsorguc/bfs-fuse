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

typedef struct
{
    char name[FILENAME_LEN];
    int inode_num;
} DirectoryEntry;

typedef struct
{
    int size;
    int block_pointers[8];
} Inode;

int fd_disk;
DirectoryEntry directory[MAX_FILES];
Inode inodes[MAX_FILES];

// Utility to find directory entry by path
int find_file(const char *name)
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, name) == 0)
        {
            return i;
        }
    }
    return -1;
}

// Get file attributes
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

// List files in directory
static int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
                       struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    (void)offset;
    (void)fi;
    (void)flags;

    filler(buf, ".", NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num > 0)
        {
            filler(buf, directory[i].name, NULL, 0, 0);
        }
    }
    return 0;
}

// Create a file
static int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    for (int i = 0; i < MAX_FILES; i++)
    {
        if (directory[i].inode_num == 0)
        {
            strncpy(directory[i].name, path + 1, FILENAME_LEN);
            directory[i].inode_num = i + 1;
            inodes[i].size = 0;
            return 0;
        }
    }
    return -ENOSPC;
}

// Delete a file
static int bfs_unlink(const char *path)
{
    int idx = find_file(path + 1);
    if (idx != -1)
    {
        directory[idx].inode_num = 0;
        memset(&inodes[directory[idx].inode_num - 1], 0, sizeof(Inode));
        return 0;
    }
    return -ENOENT;
}

// Rename a file
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

// Open file
static int bfs_open(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

// Read file
static int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    return size; // Placeholder
}

// Write file
static int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    return size; // Placeholder
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
};

int main(int argc, char *argv[])
{
    fd_disk = open("disk1", O_RDWR);
    return fuse_main(argc, argv, &bfs_oper, NULL);
}
