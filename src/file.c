#include <csvtomato.h>

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
