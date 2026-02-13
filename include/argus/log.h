#ifndef ARGUS_LOG_H
#define ARGUS_LOG_H

#include <stdio.h>
#include <stdbool.h>

/*
 * Argus ODBC Driver Logging System
 *
 * Thread-safe logging with configurable levels and output.
 * Initialized from connection string (LogLevel, LogFile) or
 * environment variables (ARGUS_LOG_LEVEL, ARGUS_LOG_FILE).
 */

/* Log levels (0 = OFF, 6 = TRACE) */
typedef enum {
    ARGUS_LOG_OFF   = 0,
    ARGUS_LOG_FATAL = 1,
    ARGUS_LOG_ERROR = 2,
    ARGUS_LOG_WARN  = 3,
    ARGUS_LOG_INFO  = 4,
    ARGUS_LOG_DEBUG = 5,
    ARGUS_LOG_TRACE = 6
} argus_log_level_t;

/* Initialize logging system (called on library load) */
void argus_log_init(void);

/* Cleanup logging system (called on library unload) */
void argus_log_cleanup(void);

/* Set log level (0-6) */
void argus_log_set_level(int level);

/* Get current log level */
int argus_log_get_level(void);

/* Set log file path (NULL for stderr) */
void argus_log_set_file(const char *path);

/* Core logging function */
void argus_log_write(argus_log_level_t level, const char *file, int line,
                     const char *func, const char *fmt, ...);

/* Check if a level is enabled (inline for performance) */
static inline bool argus_log_enabled(argus_log_level_t level) {
    extern argus_log_level_t g_argus_log_level;
    return level <= g_argus_log_level;
}

/* Convenience macros */
#define ARGUS_LOG_FATAL(...) \
    do { if (argus_log_enabled(ARGUS_LOG_FATAL)) \
        argus_log_write(ARGUS_LOG_FATAL, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#define ARGUS_LOG_ERROR(...) \
    do { if (argus_log_enabled(ARGUS_LOG_ERROR)) \
        argus_log_write(ARGUS_LOG_ERROR, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#define ARGUS_LOG_WARN(...) \
    do { if (argus_log_enabled(ARGUS_LOG_WARN)) \
        argus_log_write(ARGUS_LOG_WARN, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#define ARGUS_LOG_INFO(...) \
    do { if (argus_log_enabled(ARGUS_LOG_INFO)) \
        argus_log_write(ARGUS_LOG_INFO, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#define ARGUS_LOG_DEBUG(...) \
    do { if (argus_log_enabled(ARGUS_LOG_DEBUG)) \
        argus_log_write(ARGUS_LOG_DEBUG, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#define ARGUS_LOG_TRACE(...) \
    do { if (argus_log_enabled(ARGUS_LOG_TRACE)) \
        argus_log_write(ARGUS_LOG_TRACE, __FILE__, __LINE__, __func__, __VA_ARGS__); } while(0)

#endif /* ARGUS_LOG_H */
