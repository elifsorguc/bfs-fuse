/*
 * BFS File System Implementation using FUSE
 * The file system operates in user-space and simulates a disk using a Linux file.
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
#include <time.h>

#define BLOCK_SIZE 4096
#define MAX_FILES 128
#define FILENAME_LEN 48
#define TOTAL_BLOCKS 4096
#define DIRECT_BLOCKS 8
#define MAX_FILE_SIZE ((DIRECT_BLOCKS + BLOCK_SIZE / sizeof(int)) * BLOCK_SIZE)
#define DEBUG 1
#define MAX_OPEN_FILES 128

#define FUSE_ERROR_MSG(err) \
    ((err == -EINVAL) ? "Invalid Argument" : \
     (err == -ENOENT) ? "No Such File or Directory" : \
     (err == -ENOSPC) ? "No Space Left on Device" : \
     "Unknown Error")

#define DEBUG_PRINT(fmt, ...) \
    do { if (DEBUG) { fprintf(stderr, "DEBUG: " fmt, ##__VA_ARGS__); fflush(stderr); } } while (0)

// Data Structures
typedef struct {
    char name[FILENAME_LEN];
    int inode_num;
    int is_dir; // 1 if directory, 0 if file
} DirectoryEntry;

typedef struct {
    int size;
    int block_pointers[DIRECT_BLOCKS];
    int indirect_pointer;
    time_t creation_time;
    time_t modification_time;
    mode_t permissions;
    int ref_count; // Reference count for hard links
} Inode;


// Global Variables
int fd_disk;                          // File descriptor for disk
DirectoryEntry directory[MAX_FILES];  // Directory structure
Inode inodes[MAX_FILES];              // Inode structure
int open_file_table[MAX_OPEN_FILES] = {0};

// Function Prototypes
int read_block(int fd_disk, void *block, int k);
int write_block(int fd_disk, void *block, int k);
int find_free_block();
void update_bitmap(int block_num, int value);
int allocate_indirect_block(int *indirect_pointer);
void initialize_inodes_and_directory();
int find_file(const char *name);
void save_metadata();
static int bfs_unlink(const char *path);
static int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi);

// Disk I/O Functions
int read_block(int fd_disk, void *block, int k) {
    if (lseek(fd_disk, k * BLOCK_SIZE, SEEK_SET) == -1) {
        perror("Read block seek failed");
        return -1;
    }
    if (read(fd_disk, block, BLOCK_SIZE) != BLOCK_SIZE) {
        perror("Read block failed");
        return -1;
    }
    return 0;
}


int write_block(int fd_disk, void *block, int k) {
    lseek(fd_disk, k * BLOCK_SIZE, SEEK_SET);
    return write(fd_disk, block, BLOCK_SIZE) == BLOCK_SIZE ? 0 : -1;
}

// Bitmap Management
int find_free_block() {
    char bitmap[BLOCK_SIZE];
    if (read_block(fd_disk, bitmap, 1) != 0) return -1;

    for (int i = 0; i < TOTAL_BLOCKS; i++) {
        int byte = i / 8, bit = i % 8;
        if (!(bitmap[byte] & (1 << bit))) {
            bitmap[byte] |= (1 << bit);
            if (write_block(fd_disk, bitmap, 1) != 0) return -1;
            return i;
        }
    }
    return -1; // No free blocks
}

void update_bitmap(int block_num, int value) {
    char bitmap[BLOCK_SIZE];
    if (read_block(fd_disk, bitmap, 1) != 0) return;

    int byte = block_num / 8, bit = block_num % 8;
    if (value) bitmap[byte] |= (1 << bit); // Mark block as used
    else bitmap[byte] &= ~(1 << bit);      // Mark block as free

    write_block(fd_disk, bitmap, 1);
}

// Indirect Block Allocation
int allocate_indirect_block(int *indirect_pointer) {
    if (*indirect_pointer == 0) {
        int block = find_free_block();
        if (block == -1) return -1;

        char indirect_block[BLOCK_SIZE] = {0};
        if (write_block(fd_disk, indirect_block, block) != 0) return -1;
        *indirect_pointer = block;
    }
    return 0;
}

// Initialization
void initialize_inodes_and_directory() {
    if (read_block(fd_disk, directory, 4) == 0 && read_block(fd_disk, inodes, 5) == 0) {
        printf("Metadata loaded from disk.\n");
    } else {
        // First-time initialization if loading from disk fails
        printf("Initializing metadata for the first time.\n");
        memset(directory, 0, sizeof(directory));
        memset(inodes, 0, sizeof(inodes));

        // Root directory setup
        strcpy(directory[0].name, ".");
        directory[0].inode_num = 1;

        strcpy(directory[1].name, "..");
        directory[1].inode_num = 1;

        inodes[0].creation_time = inodes[0].modification_time = time(NULL);
    }
}


// Find a File by Name
int find_file(const char *name) {
    if (strcmp(name, "/") == 0 || strcmp(name, "") == 0) {
        return 0; // Root directory is always at index 0
    }

    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num > 0 && strcmp(directory[i].name, name) == 0) {
            fprintf(stderr, "FIND FILE: Found %s at index=%d\n", name, i);
            return i;
        }
    }

    fprintf(stderr, "FIND FILE ERROR: %s not found\n", name);
    return -1;
}



// FUSE Operations
static int bfs_getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    fprintf(stderr, "GETATTR: path=%s\n", path);
    memset(stbuf, 0, sizeof(struct stat));

    if (strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        fprintf(stderr, "GETATTR: Root directory\n");
        return 0;
    }

    int idx = find_file(path + 1);
    if (idx == -1) {
        fprintf(stderr, "GETATTR ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    stbuf->st_mode = (directory[idx].is_dir ? S_IFDIR : S_IFREG) | inode->permissions;
    stbuf->st_nlink = 1;
    stbuf->st_size = inode->size;
    stbuf->st_atime = inode->creation_time;
    stbuf->st_mtime = inode->modification_time;
    stbuf->st_ctime = inode->modification_time;

    fprintf(stderr, "GETATTR: Found file: %s, mode=%o, size=%ld\n", path, stbuf->st_mode, stbuf->st_size);
    return 0;
}


int bfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    fprintf(stderr, "READDIR: Listing directory contents, path=%s\n", path);

    // Kök dizin gibi özel durumları kontrol et
    if (strcmp(path, "/") == 0) {
        filler(buf, ".", NULL, 0); // Geçerli dizin
        filler(buf, "..", NULL, 0); // Üst dizin
    }

    // Dizin içeriğini al
    Directory *dir = find_directory_metadata(path);
    if (!dir) {
        fprintf(stderr, "READDIR ERROR: Directory not found, path=%s\n", path);
        return -ENOENT; // Hata: Dizin bulunamadı
    }

    // Dizin içeriğini döndür
    for (int i = 0; i < dir->file_count; i++) {
        fprintf(stderr, "READDIR: Found entry=%s\n", dir->files[i].name);
        filler(buf, dir->files[i].name, NULL, 0);
    }

    fprintf(stderr, "READDIR: Directory listing completed, path=%s\n", path);
    return 0; // Başarı
}




static int bfs_create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    fprintf(stderr, "CREATE: path=%s, mode=%o\n", path, mode);

    // Ayrıştırma
    const char *base_name = strrchr(path, '/');
    if (!base_name || base_name == path) {
        base_name = path + 1; // Root dizininde dosya oluşturuluyor
        fprintf(stderr, "CREATE: Adjusted base_name to=%s\n", base_name);
    } else {
        base_name++;
    }

    char parent_path[FILENAME_LEN];
    if (base_name - path > 1) {
        strncpy(parent_path, path, base_name - path - 1);
        parent_path[base_name - path - 1] = '\0';
    } else {
        strcpy(parent_path, "/");
    }

    fprintf(stderr, "CREATE: Parent path=%s, Base name=%s\n", parent_path, base_name);

    // Parent directory kontrolü
    int parent_idx = find_file(parent_path + 1);
    if (parent_idx == -1 || !directory[parent_idx].is_dir) {
        fprintf(stderr, "CREATE ERROR: Parent directory not found or not a directory. Parent path=%s\n", parent_path);
        return -ENOENT;
    }

    // Dosya zaten var mı?
    int idx = find_file(path + 1);
    if (idx != -1) {
        fprintf(stderr, "CREATE ERROR: File or directory already exists. Path=%s\n", path);
        return -EEXIST;
    }

    // Yeni dosya oluşturma
    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num == 0) {
            strncpy(directory[i].name, base_name, FILENAME_LEN);
            directory[i].inode_num = i + 1;
            directory[i].is_dir = (mode & S_IFDIR) ? 1 : 0;

            Inode *inode = &inodes[i];
            memset(inode, 0, sizeof(Inode));
            inode->creation_time = inode->modification_time = time(NULL);
            inode->permissions = mode;
            inode->ref_count = 1;

            // Parent directory'e ekleme
            Inode *parent_inode = &inodes[directory[parent_idx].inode_num - 1];
            char parent_block[BLOCK_SIZE];
            if (read_block(fd_disk, parent_block, parent_inode->block_pointers[0]) != 0) {
                fprintf(stderr, "CREATE ERROR: Failed to read parent directory block.\n");
                return -EIO;
            }
            DirectoryEntry *parent_entries = (DirectoryEntry *)parent_block;

            for (int j = 0; j < BLOCK_SIZE / sizeof(DirectoryEntry); j++) {
                if (parent_entries[j].inode_num == 0) {
                    parent_entries[j] = directory[i];
                    write_block(fd_disk, parent_block, parent_inode->block_pointers[0]);
                    save_metadata();
                    fprintf(stderr, "CREATE: File created successfully: %s\n", path);
                    return 0;
                }
            }

            fprintf(stderr, "CREATE ERROR: Parent directory is full.\n");
            return -ENOSPC;
        }
    }

    fprintf(stderr, "CREATE ERROR: No space for new file.\n");
    return -ENOSPC;
}


static int bfs_mkdir(const char *path, mode_t mode) {
    fprintf(stderr, "MKDIR: Attempting to create directory at path=%s, mode=%o\n", path, mode);

    int result = bfs_create(path, mode | S_IFDIR, NULL);
    if (result == 0) {
        fprintf(stderr, "MKDIR: Directory created successfully at path=%s\n", path);
    } else {
        fprintf(stderr, "MKDIR: Failed to create directory at path=%s, error=%d\n", path, result);
    }
    return result;
}



int bfs_rmdir(const char *path) {
    fprintf(stderr, "RMDIR: Attempting to remove directory at path=%s\n", path);

    // Kontrol: Dizin boş mu?
    if (!is_directory_empty(path)) {
        fprintf(stderr, "RMDIR ERROR: Directory not empty, path=%s\n", path);
        return -ENOTEMPTY; // Hata: Dizin boş değil
    }

    // Metadata'dan dizini kaldır
    if (!remove_directory_metadata(path)) {
        fprintf(stderr, "RMDIR ERROR: Failed to remove directory metadata, path=%s\n", path);
        return -EIO; // Hata: Giriş/Çıkış hatası
    }

    // Dizin fiziksel olarak kaldırıldı
    fprintf(stderr, "RMDIR: Directory removed successfully, path=%s\n", path);
    return 0; // Başarı
}

bool is_directory_empty(const char *path) {
    Directory *dir = find_directory_metadata(path);
    if (!dir) return false; // Dizin yoksa boş değil gibi davran

    return dir->file_count == 0; // Dizin dosya içermiyorsa boş
}

bool remove_directory_metadata(const char *path) {
    Directory *dir = find_directory_metadata(path);
    if (!dir) return false;

    // Metadata'dan dizini kaldır
    return delete_directory_metadata(path);
}

bool remove_file_metadata(const char *path) {
    File *file = find_file_metadata(path);
    if (!file) return false;

    // Metadata'dan dosyayı kaldır
    return delete_file_metadata(path);
}


static int bfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) return -ENOENT;

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    if (offset >= inode->size) return 0; // Offset beyond EOF

    size_t bytes_read = 0;
    size_t block_idx = offset / BLOCK_SIZE;
    size_t block_offset = offset % BLOCK_SIZE;

    while (bytes_read < size && block_idx < DIRECT_BLOCKS) {
        char block[BLOCK_SIZE];
        read_block(fd_disk, block, inode->block_pointers[block_idx]);

        size_t bytes_to_copy = BLOCK_SIZE - block_offset;
        if (bytes_to_copy > size - bytes_read) bytes_to_copy = size - bytes_read;

        memcpy(buf + bytes_read, block + block_offset, bytes_to_copy);
        bytes_read += bytes_to_copy;
        block_idx++;
        block_offset = 0;
    }

    return bytes_read;
}


static int bfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) return -ENOENT;

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    if (offset + size > MAX_FILE_SIZE) return -EFBIG;

    size_t bytes_written = 0;
    while (bytes_written < size) {
        size_t block_idx = offset / BLOCK_SIZE;
        size_t block_offset = offset % BLOCK_SIZE;

        int block_num;
        if (block_idx < DIRECT_BLOCKS) {
            if (inode->block_pointers[block_idx] == 0) {
                block_num = find_free_block();
                if (block_num == -1) return -ENOSPC;
                inode->block_pointers[block_idx] = block_num;
            } else {
                block_num = inode->block_pointers[block_idx];
            }
        } else {
            if (allocate_indirect_block(&inode->indirect_pointer) == -1) return -ENOSPC;

            char indirect_block[BLOCK_SIZE];
            read_block(fd_disk, indirect_block, inode->indirect_pointer);
            int *indirect_pointers = (int *)indirect_block;

            block_num = indirect_pointers[block_idx - DIRECT_BLOCKS];
            if (block_num == 0) {
                block_num = find_free_block();
                if (block_num == -1) return -ENOSPC;
                indirect_pointers[block_idx - DIRECT_BLOCKS] = block_num;
                write_block(fd_disk, indirect_block, inode->indirect_pointer);
            }
        }

        char block[BLOCK_SIZE];
        read_block(fd_disk, block, block_num);
        size_t bytes_to_write = (BLOCK_SIZE - block_offset > size - bytes_written) ? (size - bytes_written) : (BLOCK_SIZE - block_offset);

        memcpy(block + block_offset, buf + bytes_written, bytes_to_write);
        write_block(fd_disk, block, block_num);

        bytes_written += bytes_to_write;
        offset += bytes_to_write;
    }

    inode->size = (offset > inode->size) ? offset : inode->size;
    inode->modification_time = time(NULL);
    return bytes_written;
}

static int bfs_open(const char *path, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) return -ENOENT;

    // Check if file can be added to open file table
    for (int i = 0; i < MAX_OPEN_FILES; i++) {
        if (open_file_table[i] == 0) {
            open_file_table[i] = directory[idx].inode_num;
            return 0; // File opened successfully
        }
    }
    return -EMFILE; // Too many open files
}

static int bfs_release(const char *path, struct fuse_file_info *fi) {
    int idx = find_file(path + 1);
    if (idx == -1) return -ENOENT;

    for (int i = 0; i < MAX_OPEN_FILES; i++) {
        if (open_file_table[i] == directory[idx].inode_num) {
            open_file_table[i] = 0; // Mark as closed
            return 0;
        }
    }
    return -EBADF; // File was not open
}

static int bfs_unlink(const char *path) {
    fprintf(stderr, "UNLINK: Attempting to delete file at path=%s\n", path);

    int file_idx = find_file(path + 1);
    if (file_idx == -1) {
        fprintf(stderr, "UNLINK ERROR: File not found: %s\n", path);
        return -ENOENT;
    }

    // Doğru indeks aralığında olduğundan emin olun
    if (file_idx < 0 || file_idx >= MAX_FILES) {
        fprintf(stderr, "UNLINK ERROR: Invalid file index: %d\n", file_idx);
        return -EINVAL;
    }

    // İlgili inode ve dizin girişini sıfırla
    int inode_idx = directory[file_idx].inode_num - 1;
    if (inode_idx < 0 || inode_idx >= MAX_FILES) {
        fprintf(stderr, "UNLINK ERROR: Invalid inode index: %d\n", inode_idx);
        return -EINVAL;
    }

    memset(&directory[file_idx], 0, sizeof(DirectoryEntry));
    memset(&inodes[inode_idx], 0, sizeof(Inode));

    // Metadata'yı kaydet
    save_metadata();

    fprintf(stderr, "UNLINK: File deleted successfully at path=%s\n", path);
    return 0;
}





static int bfs_link(const char *from, const char *to) {
    int from_idx = find_file(from + 1);
    if (from_idx == -1) return -ENOENT;

    int to_idx = find_file(to + 1);
    if (to_idx != -1) return -EEXIST;

    for (int i = 0; i < MAX_FILES; i++) {
        if (directory[i].inode_num == 0) {
            // Create new directory entry pointing to the same inode
            strncpy(directory[i].name, to + 1, FILENAME_LEN);
            directory[i].inode_num = directory[from_idx].inode_num;
            inodes[directory[i].inode_num - 1].ref_count++;
            save_metadata();
            return 0;
        }
    }
    return -ENOSPC;
}

static int bfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
    (void)fi; // Unused parameter
    int idx = find_file(path + 1);
    if (idx == -1) return -ENOENT;

    Inode *inode = &inodes[directory[idx].inode_num - 1];
    inode->creation_time = tv[0].tv_sec;       // Update access time
    inode->modification_time = tv[1].tv_sec;  // Update modification time
    return 0;
}



static int bfs_rename(const char *from, const char *to, unsigned int flags) {
    fprintf(stderr, "RENAME: Attempting to rename from=%s to=%s\n", from, to);

    int from_idx = find_file(from + 1);
    if (from_idx == -1) {
        fprintf(stderr, "RENAME ERROR: Source file not found: %s\n", from);
        return -ENOENT;
    }

    int to_idx = find_file(to + 1);
    if (to_idx != -1) {
        // Eğer hedef dosya varsa, onu sil
        bfs_unlink(to);
    }

    // Kaynak girişini güncelle
    strncpy(directory[from_idx].name, to + 1, FILENAME_LEN);
    save_metadata(); // Metadata'yı güncelle

    fprintf(stderr, "RENAME: Successfully renamed from=%s to=%s\n", from, to);
    return 0;
}

static int bfs_access(const char *path, int mask) {
    DEBUG_PRINT("ACCESS: path=%s, mask=%d\n", path, mask);
    return 0; // Allow all access
}

void save_metadata() {
    write_block(fd_disk, directory, 4);       // Save directory to disk
    write_block(fd_disk, inodes, 5);          // Save inodes to disk
    printf("Metadata saved successfully.\n");
}
static struct fuse_operations bfs_oper = {
    .getattr = bfs_getattr,
    .readdir = bfs_readdir,
    .create = bfs_create,
    .unlink = bfs_unlink,
    .rename = bfs_rename,
    .open = bfs_open,
    .write = bfs_write, // Added write
    .read = bfs_read,   // Added read
    .release = bfs_release, // Added release
    .utimens = bfs_utimens,
    .mkdir = bfs_mkdir,
    .rmdir = bfs_rmdir,
    .link = bfs_link,
    .access = bfs_access,
};


int main(int argc, char *argv[]) {
    fd_disk = open("disk1", O_RDWR);
    if (fd_disk < 0) {
        perror("Failed to open disk file");
        return 1;
    }
    printf("Disk file 'disk1' opened successfully.\n");

    initialize_inodes_and_directory();

    printf("Mounting filesystem...\n");
    int ret = fuse_main(argc, argv, &bfs_oper, NULL);

    if (ret != 0) {
        fprintf(stderr, "Error: FUSE failed to initialize or encountered an error.\n");
    } else {
        printf("Filesystem unmounted successfully.\n");
    }

    save_metadata();
    close(fd_disk);
    return ret;
}
