#ifndef __LOG_H__
#define __LOG_H__

typedef enum {
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_INFO,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL
} log_level;

int log_debug(char *fmt, ...);
int log_info(char *fmt, ...);
int log_warning(char *fmt, ...);
int log_error(char *fmt, ...);
int log_fatal(char *fmt, ...);

void set_log_lvl(log_level lvl);

#endif
