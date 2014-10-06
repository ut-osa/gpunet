#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <pthread.h>

#include <rdma/log.h>

log_level cur_log_level = LOG_LEVEL_DEBUG;

void set_log_lvl(log_level lvl) {
    cur_log_level = lvl;
}

#define LOG_BUF_SIZE 8192

/* locking for log buf */
pthread_mutex_t _log_lock = PTHREAD_MUTEX_INITIALIZER;

static int log_common(log_level lvl, const char* prefix, char* fmt, va_list ap, int exit_status) {
    int ret = 0, ret2 = 0;
    static char buf[LOG_BUF_SIZE];

    if (cur_log_level > lvl) {
        return 0;
    }
    
    pthread_mutex_lock(&_log_lock);
    if (prefix) {
        ret = snprintf(buf, LOG_BUF_SIZE, "[%s]%c", prefix, exit_status ? '*' : ' ');
        if (ret < 0)
            log_fatal("snprintf ret %d", ret);
    }

    ret2 = vsnprintf(buf + ret, LOG_BUF_SIZE - ret, fmt, ap);
    if (ret2 < 0)
        log_fatal("snprintf ret2 %d", ret2);
    
    if (ret + ret2 < LOG_BUF_SIZE - 1) {
        buf[ret + ret2] = '\n';
        buf[ret + ret2 + 1] = '\0';
    } else {
        buf[LOG_BUF_SIZE - 2] = '\n';
        buf[LOG_BUF_SIZE - 1] = '\0';
    }
    
    fputs(buf, stderr);
    pthread_mutex_unlock(&_log_lock);

    if (exit_status) {
        exit(1);
    }
    
    return (ret >= 0) ? 0 : ret + ret2 + 1;
}

#define DO_LOG(_log_level, prefix, _is_exit)                            \
    do {                                                                \
        va_list ap;                                                     \
        va_start(ap, fmt);                                              \
        int ret = log_common(_log_level, prefix, fmt, ap, _is_exit);    \
        va_end(ap);                                                     \
        return ret;                                                     \
    } while (0)

#define LOG_DEF(func_lvl, lvl, exit)                  \
    int log_##func_lvl(char* fmt, ...) {              \
        DO_LOG(LOG_LEVEL_##lvl, NULL, exit);          \
    }

#define LOG_PREFIX_DEF(func_lvl, lvl, exit)                      \
    int log_prefix_##func_lvl(char* prefix, char* fmt, ...) {    \
        DO_LOG(LOG_LEVEL_##lvl, prefix, exit);                   \
    }

#define DEF_LOG_FUNCS(_macro)                \
    _macro(debug, DEBUG, 0)                  \
    _macro(info, INFO, 0)                    \
    _macro(warning, WARNING, 0)              \
    _macro(error, ERROR, 1)                  \
    _macro(fatal, FATAL, 1)

/* log_{debug/info/warning/error/fatal} */
DEF_LOG_FUNCS(LOG_DEF)

/* log_prefix_{debug/info/warning/error/fatal} */
DEF_LOG_FUNCS(LOG_PREFIX_DEF)

