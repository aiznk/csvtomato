#include "csvtomato.h"

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
