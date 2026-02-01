#include <csvtomato.h>

void
csvtmt_dir_node_del(CsvTomatoDirNode *self) {
    if (self) {
        free(self);
    }
}

CsvTomatoDirNode *
csvtmt_dir_node_new(void) {
    CsvTomatoDirNode *self = calloc(1, sizeof(CsvTomatoDirNode));
    if (!self) {
        return NULL;
    }
    return self;
}

const char *
csvtmt_dir_node_name(const CsvTomatoDirNode *self) {
    if (!self) {
        return NULL;
    }

#if defined(LANG1_FILE__WINDOWS)
    return self->finddata.cFileName;
#else
    return self->node->d_name;
#endif
}

int
csvtmt_dir_close(CsvTomatoDir *self) {
    if (self) {
        int ret = 0;
#if defined(_WIN32)
        if (self->handle) {
            ret = FindClose(self->handle);
            if (ret == 0) {
                // Error
            }
            self->handle = NULL;
            ret = !ret;
        } else {
            ret = -1;
        }
#else
        ret = closedir(self->directory);
        if (ret != 0) {
            // Error
        }
#endif
        free(self);

        return ret;
    }

    return -1;
}

CsvTomatoDir *
csvtmt_dir_open(const char *path) {
    if (!path) {
        return NULL;
    }

    CsvTomatoDir *self = calloc(1, sizeof(CsvTomatoDir));
    if (!self) {
        return NULL;
    }

#if defined(_WIN32)
    if (!csvtmt_file_exists(path)) {
        return NULL;
    }
    self->handle = NULL;
    snprintf(self->dirpath, sizeof(self->dirpath), "%s/*", path);
#else
    if (!(self->directory = opendir(path))) {
        free(self);
        return NULL;
    }
#endif
    return self;
}

CsvTomatoDirNode *
csvtmt_dir_read(CsvTomatoDir *self) {
    if (!self) {
        return NULL;
    }

    CsvTomatoDirNode *node = csvtmt_dir_node_new();
    if (!node) {
        return NULL;
    }

#if defined(_WIN32)
    if (!self->handle) {
        if ((self->handle = FindFirstFile(self->dirpath, &node->finddata)) == INVALID_HANDLE_VALUE) {
            csvtmt_dir_node_del(node);
            return NULL;

        }

    } else {
        if (!FindNextFile(self->handle, &node->finddata)) {
            csvtmt_dir_node_del(node);
            return NULL; // Done to find
        }
    }

#else
    errno = 0;
    if (!(node->node = readdir(self->directory))) {
        if (errno != 0) {
            csvtmt_dir_node_del(node);
            return NULL;
        } else {
            // Done to readdir
            csvtmt_dir_node_del(node);
            return NULL;
        }
    }
#endif
    return node;
}

char *
csvtmt_file_read(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    char *p = malloc(size+1);
    if (!p) {
        fclose(fp);
        return NULL;
    }

    fread(p, sizeof(char), size, fp);
    p[size] = 0;

    fclose(fp);
    return p;
}

int
csvtmt_file_rename(const char *old, const char *new) {
    return rename(old, new);
}

/// touch 相当の処理
/// 成功: 0, 失敗: -1
int csvtmt_file_touch(const char *path) {
    if (!path || !*path) return -1;

    if (!csvtmt_file_exists(path)) {
        // ファイルが存在しない場合 → 新規作成
        FILE *fp = fopen(path, "wb");
        if (!fp) return -1;
        fclose(fp);
        return 0;
    }

    // ファイルが存在する場合 → タイムスタンプ更新
#ifdef _WIN32
    HANDLE hFile = CreateFileA(path,
                               FILE_WRITE_ATTRIBUTES,
                               FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                               NULL,
                               OPEN_EXISTING,
                               FILE_ATTRIBUTE_NORMAL,
                               NULL);
    if (hFile == INVALID_HANDLE_VALUE) return -1;

    FILETIME ft;
    GetSystemTimeAsFileTime(&ft); // 現在時刻を取得
    int ret = SetFileTime(hFile, NULL, &ft, &ft) ? 0 : -1;

    CloseHandle(hFile);
    return ret;
#else
    // utime(NULL) で現在時刻に更新
    return utime(path, NULL);
#endif
}

/// ファイルまたはディレクトリが存在するかを確認
int csvtmt_file_exists(const char *path) {
    if (!path) return 0;
#ifdef _WIN32
    DWORD attrib = GetFileAttributesA(path);
    return (attrib != INVALID_FILE_ATTRIBUTES);
#else
    return (access(path, F_OK) == 0);
#endif
}

/// ディレクトリを作成する（途中の親ディレクトリも作成）
/// 成功: 0, 失敗: -1
int csvtmt_file_mkdir(const char *path) {
    if (!path || !*path) {
        return -1;
    }

    char tmp[1024];
    strncpy(tmp, path, sizeof(tmp) - 1);
    tmp[sizeof(tmp) - 1] = '\0';

    size_t len = strlen(tmp);
    if (tmp[len - 1] == '/' || tmp[len - 1] == '\\') {
        tmp[len - 1] = '\0';  // 末尾の / \ を削除
    }

    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/' || *p == '\\') {
            *p = '\0';
            if (!csvtmt_file_exists(tmp)) {
                if (CSVTMT_MKDIR(tmp) != 0 && errno != EEXIST) {
                    return -1; 
                }
            }
            *p = '/'; // 戻す（Windowsでも / は許容される）
        }
    }

    if (!csvtmt_file_exists(tmp)) {
        if (CSVTMT_MKDIR(tmp) != 0 && errno != EEXIST) {
            return -1;
        }
    }
    return 0;
}

int 
csvtmt_file_remove(const char *path) {
    return remove(path);
}
