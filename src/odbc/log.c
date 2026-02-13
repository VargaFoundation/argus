/*
 * log.c - Thread-safe logging system for Argus ODBC driver
 */

#include "argus/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

/* Global log state */
argus_log_level_t g_argus_log_level = ARGUS_LOG_OFF;
static FILE *g_argus_log_file = NULL;
static char *g_argus_log_file_path = NULL;

#ifdef _WIN32
static CRITICAL_SECTION g_argus_log_mutex;
static bool g_argus_log_mutex_initialized = false;
#else
static pthread_mutex_t g_argus_log_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

/* Level names for output */
static const char *level_names[] = {
    "OFF",
    "FATAL",
    "ERROR",
    "WARN",
    "INFO",
    "DEBUG",
    "TRACE"
};

void argus_log_init(void)
{
#ifdef _WIN32
    if (!g_argus_log_mutex_initialized) {
        InitializeCriticalSection(&g_argus_log_mutex);
        g_argus_log_mutex_initialized = true;
    }
#endif

    /* Check environment variables for defaults */
    const char *env_level = getenv("ARGUS_LOG_LEVEL");
    if (env_level) {
        int level = atoi(env_level);
        if (level >= ARGUS_LOG_OFF && level <= ARGUS_LOG_TRACE) {
            g_argus_log_level = level;
        }
    }

    const char *env_file = getenv("ARGUS_LOG_FILE");
    if (env_file && env_file[0]) {
        argus_log_set_file(env_file);
    }
}

void argus_log_cleanup(void)
{
#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        EnterCriticalSection(&g_argus_log_mutex);
    }
#else
    pthread_mutex_lock(&g_argus_log_mutex);
#endif

    if (g_argus_log_file && g_argus_log_file != stderr) {
        fclose(g_argus_log_file);
        g_argus_log_file = NULL;
    }

    if (g_argus_log_file_path) {
        free(g_argus_log_file_path);
        g_argus_log_file_path = NULL;
    }

#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        LeaveCriticalSection(&g_argus_log_mutex);
        DeleteCriticalSection(&g_argus_log_mutex);
        g_argus_log_mutex_initialized = false;
    }
#else
    pthread_mutex_unlock(&g_argus_log_mutex);
#endif
}

void argus_log_set_level(int level)
{
    if (level >= ARGUS_LOG_OFF && level <= ARGUS_LOG_TRACE) {
        g_argus_log_level = level;
    }
}

int argus_log_get_level(void)
{
    return g_argus_log_level;
}

void argus_log_set_file(const char *path)
{
#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        EnterCriticalSection(&g_argus_log_mutex);
    }
#else
    pthread_mutex_lock(&g_argus_log_mutex);
#endif

    /* Close old file if not stderr */
    if (g_argus_log_file && g_argus_log_file != stderr) {
        fclose(g_argus_log_file);
        g_argus_log_file = NULL;
    }

    /* Free old path */
    if (g_argus_log_file_path) {
        free(g_argus_log_file_path);
        g_argus_log_file_path = NULL;
    }

    /* Open new file or use stderr */
    if (path && path[0]) {
        FILE *fp = fopen(path, "a");
        if (fp) {
            g_argus_log_file = fp;
            g_argus_log_file_path = strdup(path);
            /* Disable buffering for immediate flush */
            setvbuf(fp, NULL, _IONBF, 0);
        } else {
            /* Fallback to stderr on error */
            g_argus_log_file = stderr;
        }
    } else {
        g_argus_log_file = stderr;
    }

#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        LeaveCriticalSection(&g_argus_log_mutex);
    }
#else
    pthread_mutex_unlock(&g_argus_log_mutex);
#endif
}

void argus_log_write(argus_log_level_t level, const char *file, int line,
                     const char *func, const char *fmt, ...)
{
    if (level > g_argus_log_level || level > ARGUS_LOG_TRACE) {
        return;
    }

#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        EnterCriticalSection(&g_argus_log_mutex);
    }
#else
    pthread_mutex_lock(&g_argus_log_mutex);
#endif

    FILE *out = g_argus_log_file ? g_argus_log_file : stderr;

    /* Timestamp */
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);

    /* Extract just the filename (not full path) */
    const char *filename = strrchr(file, '/');
    if (!filename) {
        filename = strrchr(file, '\\');
    }
    filename = filename ? filename + 1 : file;

    /* Print log prefix */
    fprintf(out, "[%s] [%-5s] [%s:%d %s] ",
            timestamp,
            level_names[level],
            filename,
            line,
            func);

    /* Print message */
    va_list args;
    va_start(args, fmt);
    vfprintf(out, fmt, args);
    va_end(args);

    fprintf(out, "\n");

#ifdef _WIN32
    if (g_argus_log_mutex_initialized) {
        LeaveCriticalSection(&g_argus_log_mutex);
    }
#else
    pthread_mutex_unlock(&g_argus_log_mutex);
#endif
}
