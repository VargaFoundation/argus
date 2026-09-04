/* SPDX-License-Identifier: Apache-2.0 */
/*
 * ODBC installer setup entry points (Windows only).
 *
 * The ODBCINST.INI "Setup" key of the driver points at this DLL, so the
 * installer library (odbccp32) loads it to create, modify and remove DSNs.
 *
 * Two paths:
 *   - scripted (PowerShell Add-OdbcDsn, odbcconf): a complete attribute list
 *     arrives and is written without any UI — unchanged behaviour;
 *   - interactive (odbcad32.exe Add/Configure with a parent window): a real
 *     configuration dialog opens — DSN, backend, host/port, database,
 *     credentials, SSL, auth mechanism — with a live "Test" button that
 *     drives this very driver's own SQLDriverConnect (the Setup entry points
 *     live in the driver DLL). This closes the long-standing gap against the
 *     commercial drivers' DSN dialogs.
 *
 * The dialog template is built in memory (DLGTEMPLATE), so no resource file
 * enters the MinGW build.
 *
 * lpszAttributes is a list of "KEY=value" entries, each terminated by a
 * null byte, the list itself terminated by a double null.
 */
#ifdef _WIN32

#include <windows.h>
#include <odbcinst.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "argus/compat.h"

#define ARGUS_SETUP_EXPORT __declspec(dllexport)

/* ── Attribute-list helpers ──────────────────────────────────── */

/* Find "key=" (case-insensitive) in the null-separated list; returns a
 * pointer to the value, or NULL. */
static const char *setup_find_attr(const char *attrs, const char *key)
{
    if (!attrs)
        return NULL;
    size_t key_len = strlen(key);
    for (const char *p = attrs; *p; p += strlen(p) + 1) {
        if (strncasecmp(p, key, key_len) == 0 && p[key_len] == '=')
            return p + key_len + 1;
    }
    return NULL;
}

/* Write every attribute except DSN and Driver into the DSN section. */
static BOOL setup_write_attrs(const char *dsn, const char *attrs)
{
    for (const char *p = attrs; *p; p += strlen(p) + 1) {
        const char *eq = strchr(p, '=');
        if (!eq || eq == p)
            continue;

        size_t key_len = (size_t)(eq - p);
        char key[256];
        if (key_len >= sizeof(key))
            continue;
        memcpy(key, p, key_len);
        key[key_len] = '\0';

        if (strcasecmp(key, "DSN") == 0 || strcasecmp(key, "Driver") == 0)
            continue;

        if (!SQLWritePrivateProfileString(dsn, key, eq + 1, "ODBC.INI"))
            return FALSE;
    }
    return TRUE;
}

/* ── Interactive configuration dialog ─────────────────────────── */

/* Control ids */
#define IDC_DSN       1001
#define IDC_BACKEND   1002
#define IDC_HOST      1003
#define IDC_PORT      1004
#define IDC_DATABASE  1005
#define IDC_UID       1006
#define IDC_PWD       1007
#define IDC_SSL       1008
#define IDC_SSLVERIFY 1009
#define IDC_AUTHMECH  1010
#define IDC_TEST      1011
#define IDC_STATUS    1012

static const char *const setup_backends[] = {
    "hive", "impala", "trino", "phoenix", "pinot",
    "druid", "bigquery", "mysql", "flightsql", NULL
};
static const char *const setup_authmechs[] = {
    "", "LDAP", "KERBEROS", "JWT", "OAUTH2", NULL
};

/* Everything the dialog edits, prefilled before DialogBoxIndirect and read
 * back on OK. */
typedef struct {
    char dsn[128];
    char backend[32];
    char host[256];
    char port[16];
    char database[128];
    char uid[128];
    char pwd[128];
    BOOL ssl;
    BOOL ssl_verify;
    char authmech[32];
    BOOL dsn_editable;      /* FALSE when reconfiguring an existing DSN */
} setup_state_t;

/* ── In-memory DLGTEMPLATE builder ────────────────────────────── */

typedef struct {
    BYTE  buf[8192];
    size_t len;
} tmpl_t;

static void t_align(tmpl_t *t) { t->len = (t->len + 3) & ~(size_t)3; }
static void t_word(tmpl_t *t, WORD w)
{
    memcpy(t->buf + t->len, &w, 2);
    t->len += 2;
}
static void t_dword(tmpl_t *t, DWORD d)
{
    memcpy(t->buf + t->len, &d, 4);
    t->len += 4;
}
static void t_wstr(tmpl_t *t, const char *s)
{
    /* ASCII -> UTF-16LE, NUL included. */
    for (; *s; s++) t_word(t, (WORD)(unsigned char)*s);
    t_word(t, 0);
}

static void t_item(tmpl_t *t, DWORD style, short x, short y, short cx,
                   short cy, WORD id, WORD cls, const char *text)
{
    t_align(t);
    t_dword(t, style | WS_CHILD | WS_VISIBLE);
    t_dword(t, 0);                       /* exstyle */
    t_word(t, (WORD)x);  t_word(t, (WORD)y);
    t_word(t, (WORD)cx); t_word(t, (WORD)cy);
    t_word(t, id);
    t_word(t, 0xFFFF); t_word(t, cls);   /* system class atom */
    t_wstr(t, text ? text : "");
    t_word(t, 0);                        /* no creation data */
}

#define CLS_BUTTON  0x0080
#define CLS_EDIT    0x0081
#define CLS_STATIC  0x0082
#define CLS_COMBO   0x0085

/* Label + control row helper coordinates (dialog units). */
#define ROW(n) (short)(8 + (n) * 20)

static const DLGTEMPLATE *build_template(tmpl_t *t, int *n_items_out)
{
    memset(t, 0, sizeof(*t));
    int items = 0;

    /* header — patched with the item count at the end */
    t_dword(t, DS_MODALFRAME | DS_SETFONT | WS_POPUP | WS_CAPTION | WS_SYSMENU);
    t_dword(t, 0);
    size_t count_at = t->len;
    t_word(t, 0);                        /* cdit, patched below */
    t_word(t, 0); t_word(t, 0);          /* x, y */
    t_word(t, 260); t_word(t, 248);      /* cx, cy */
    t_word(t, 0);                        /* no menu */
    t_word(t, 0);                        /* default class */
    t_wstr(t, "Argus ODBC Driver Setup");
    t_word(t, 8);                        /* font size */
    t_wstr(t, "MS Shell Dlg");

#define LABELED(nrow, label, id, cls, style, w)                              \
    do {                                                                     \
        t_item(t, SS_RIGHT, 6, (short)(ROW(nrow) + 2), 62, 10, (WORD)-1,     \
               CLS_STATIC, label);                                           \
        t_item(t, style, 74, ROW(nrow), w, 12, id, cls, "");                 \
        items += 2;                                                          \
    } while (0)

    LABELED(0, "Data Source:", IDC_DSN, CLS_EDIT,
            ES_AUTOHSCROLL | WS_BORDER | WS_TABSTOP, 178);
    LABELED(1, "Backend:", IDC_BACKEND, CLS_COMBO,
            CBS_DROPDOWNLIST | WS_VSCROLL | WS_TABSTOP, 178);
    LABELED(2, "Host:", IDC_HOST, CLS_EDIT,
            ES_AUTOHSCROLL | WS_BORDER | WS_TABSTOP, 178);
    LABELED(3, "Port:", IDC_PORT, CLS_EDIT,
            ES_AUTOHSCROLL | ES_NUMBER | WS_BORDER | WS_TABSTOP, 60);
    LABELED(4, "Database:", IDC_DATABASE, CLS_EDIT,
            ES_AUTOHSCROLL | WS_BORDER | WS_TABSTOP, 178);
    LABELED(5, "User:", IDC_UID, CLS_EDIT,
            ES_AUTOHSCROLL | WS_BORDER | WS_TABSTOP, 178);
    LABELED(6, "Password:", IDC_PWD, CLS_EDIT,
            ES_AUTOHSCROLL | ES_PASSWORD | WS_BORDER | WS_TABSTOP, 178);
    LABELED(7, "Auth mechanism:", IDC_AUTHMECH, CLS_COMBO,
            CBS_DROPDOWNLIST | WS_VSCROLL | WS_TABSTOP, 178);

    t_item(t, BS_AUTOCHECKBOX | WS_TABSTOP, 74, ROW(8), 80, 10,
           IDC_SSL, CLS_BUTTON, "Use SSL/TLS");
    t_item(t, BS_AUTOCHECKBOX | WS_TABSTOP, 160, ROW(8), 92, 10,
           IDC_SSLVERIFY, CLS_BUTTON, "Verify certificate");
    items += 2;

    t_item(t, 0, 6, ROW(9) + 4, 246, 20, IDC_STATUS, CLS_STATIC, "");
    items += 1;

    t_item(t, BS_PUSHBUTTON | WS_TABSTOP, 6, 226, 70, 14,
           IDC_TEST, CLS_BUTTON, "Test Connection");
    t_item(t, BS_DEFPUSHBUTTON | WS_TABSTOP, 146, 226, 52, 14,
           IDOK, CLS_BUTTON, "OK");
    t_item(t, BS_PUSHBUTTON | WS_TABSTOP, 202, 226, 52, 14,
           IDCANCEL, CLS_BUTTON, "Cancel");
    items += 3;

#undef LABELED

    WORD cdit = (WORD)items;
    memcpy(t->buf + count_at, &cdit, 2);
    *n_items_out = items;
    return (const DLGTEMPLATE *)t->buf;
}

/* ── Test-connection: drive our own driver ────────────────────── */

static void setup_read_controls(HWND dlg, setup_state_t *st)
{
    GetDlgItemTextA(dlg, IDC_DSN, st->dsn, sizeof(st->dsn));
    GetDlgItemTextA(dlg, IDC_HOST, st->host, sizeof(st->host));
    GetDlgItemTextA(dlg, IDC_PORT, st->port, sizeof(st->port));
    GetDlgItemTextA(dlg, IDC_DATABASE, st->database, sizeof(st->database));
    GetDlgItemTextA(dlg, IDC_UID, st->uid, sizeof(st->uid));
    GetDlgItemTextA(dlg, IDC_PWD, st->pwd, sizeof(st->pwd));
    GetDlgItemTextA(dlg, IDC_BACKEND, st->backend, sizeof(st->backend));
    GetDlgItemTextA(dlg, IDC_AUTHMECH, st->authmech, sizeof(st->authmech));
    st->ssl = IsDlgButtonChecked(dlg, IDC_SSL) == BST_CHECKED;
    st->ssl_verify = IsDlgButtonChecked(dlg, IDC_SSLVERIFY) == BST_CHECKED;
}

static void setup_build_connstr(const setup_state_t *st, char *out, size_t n)
{
    snprintf(out, n,
             "BACKEND=%s;HOST=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s;"
             "SSL=%d;SSLVerify=%d%s%s",
             st->backend, st->host, st->port, st->database, st->uid, st->pwd,
             st->ssl ? 1 : 0, st->ssl_verify ? 1 : 0,
             st->authmech[0] ? ";AUTHMECH=" : "",
             st->authmech);
}

static void setup_test_connection(HWND dlg, setup_state_t *st)
{
    setup_read_controls(dlg, st);
    char connstr[1024];
    setup_build_connstr(st, connstr, sizeof(connstr));

    SetDlgItemTextA(dlg, IDC_STATUS, "Connecting...");
    UpdateWindow(dlg);

    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    char msg[512] = "Connection failed.";
    BOOL ok = FALSE;

    if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env) == SQL_SUCCESS) {
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
        if (SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc) == SQL_SUCCESS) {
            SQLRETURN rc = SQLDriverConnect(dbc, NULL, (SQLCHAR *)connstr,
                                            SQL_NTS, NULL, 0, NULL,
                                            SQL_DRIVER_NOPROMPT);
            if (SQL_SUCCEEDED(rc)) {
                ok = TRUE;
                snprintf(msg, sizeof(msg), "Connection successful.");
                SQLDisconnect(dbc);
            } else {
                SQLCHAR st5[6] = "", txt[384] = "";
                SQLSMALLINT len = 0;
                SQLINTEGER nat = 0;
                SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, st5, &nat, txt,
                              sizeof(txt), &len);
                snprintf(msg, sizeof(msg), "Failed [%s] %s",
                         (char *)st5, (char *)txt);
            }
            SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        }
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }
    (void)ok;
    SetDlgItemTextA(dlg, IDC_STATUS, msg);
}

/* ── Dialog procedure ─────────────────────────────────────────── */

static INT_PTR CALLBACK setup_dlgproc(HWND dlg, UINT msg, WPARAM wp,
                                      LPARAM lp)
{
    setup_state_t *st = (setup_state_t *)GetWindowLongPtrA(dlg, GWLP_USERDATA);

    switch (msg) {
    case WM_INITDIALOG: {
        st = (setup_state_t *)lp;
        SetWindowLongPtrA(dlg, GWLP_USERDATA, (LONG_PTR)st);

        for (int i = 0; setup_backends[i]; i++)
            SendDlgItemMessageA(dlg, IDC_BACKEND, CB_ADDSTRING, 0,
                                (LPARAM)setup_backends[i]);
        for (int i = 0; setup_authmechs[i]; i++)
            SendDlgItemMessageA(dlg, IDC_AUTHMECH, CB_ADDSTRING, 0,
                                (LPARAM)(setup_authmechs[i][0]
                                             ? setup_authmechs[i]
                                             : "(none)"));

        SetDlgItemTextA(dlg, IDC_DSN, st->dsn);
        SetDlgItemTextA(dlg, IDC_HOST, st->host);
        SetDlgItemTextA(dlg, IDC_PORT, st->port);
        SetDlgItemTextA(dlg, IDC_DATABASE, st->database);
        SetDlgItemTextA(dlg, IDC_UID, st->uid);
        SetDlgItemTextA(dlg, IDC_PWD, st->pwd);
        CheckDlgButton(dlg, IDC_SSL, st->ssl ? BST_CHECKED : BST_UNCHECKED);
        CheckDlgButton(dlg, IDC_SSLVERIFY,
                       st->ssl_verify ? BST_CHECKED : BST_UNCHECKED);

        if (SendDlgItemMessageA(dlg, IDC_BACKEND, CB_SELECTSTRING,
                                (WPARAM)-1, (LPARAM)st->backend) == CB_ERR)
            SendDlgItemMessageA(dlg, IDC_BACKEND, CB_SETCURSEL, 0, 0);
        if (!st->authmech[0] ||
            SendDlgItemMessageA(dlg, IDC_AUTHMECH, CB_SELECTSTRING,
                                (WPARAM)-1, (LPARAM)st->authmech) == CB_ERR)
            SendDlgItemMessageA(dlg, IDC_AUTHMECH, CB_SETCURSEL, 0, 0);

        if (!st->dsn_editable)
            EnableWindow(GetDlgItem(dlg, IDC_DSN), FALSE);
        return TRUE;
    }

    case WM_COMMAND:
        switch (LOWORD(wp)) {
        case IDC_TEST:
            setup_test_connection(dlg, st);
            return TRUE;

        case IDOK: {
            setup_read_controls(dlg, st);
            if (SendDlgItemMessageA(dlg, IDC_AUTHMECH, CB_GETCURSEL, 0, 0)
                    == 0)
                st->authmech[0] = '\0';   /* "(none)" */
            if (!st->dsn[0] || !SQLValidDSN(st->dsn)) {
                SetDlgItemTextA(dlg, IDC_STATUS,
                                "Enter a valid data source name.");
                return TRUE;
            }
            EndDialog(dlg, IDOK);
            return TRUE;
        }

        case IDCANCEL:
            EndDialog(dlg, IDCANCEL);
            return TRUE;
        }
        return FALSE;
    }
    return FALSE;
}

/* Prefill from an existing DSN section (ODBC_CONFIG_DSN). */
static void setup_load_dsn(setup_state_t *st)
{
    SQLGetPrivateProfileString(st->dsn, "BACKEND", "hive", st->backend,
                               sizeof(st->backend), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "HOST", "", st->host,
                               sizeof(st->host), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "PORT", "", st->port,
                               sizeof(st->port), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "DATABASE", "", st->database,
                               sizeof(st->database), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "UID", "", st->uid,
                               sizeof(st->uid), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "PWD", "", st->pwd,
                               sizeof(st->pwd), "ODBC.INI");
    SQLGetPrivateProfileString(st->dsn, "AUTHMECH", "", st->authmech,
                               sizeof(st->authmech), "ODBC.INI");
    char flag[8] = "";
    SQLGetPrivateProfileString(st->dsn, "SSL", "0", flag, sizeof(flag),
                               "ODBC.INI");
    st->ssl = (flag[0] == '1' || flag[0] == 't' || flag[0] == 'T');
    SQLGetPrivateProfileString(st->dsn, "SSLVerify", "1", flag, sizeof(flag),
                               "ODBC.INI");
    st->ssl_verify = !(flag[0] == '0' || flag[0] == 'f' || flag[0] == 'F');
}

/* Prefill from the attribute list the caller passed. */
static void setup_load_attrs(setup_state_t *st, const char *attrs)
{
    const char *v;
#define GRAB(key, field)                                                     \
    do {                                                                     \
        v = setup_find_attr(attrs, key);                                     \
        if (v) snprintf(st->field, sizeof(st->field), "%s", v);              \
    } while (0)
    GRAB("DSN", dsn);
    GRAB("BACKEND", backend);
    GRAB("HOST", host);
    GRAB("PORT", port);
    GRAB("DATABASE", database);
    GRAB("UID", uid);
    GRAB("PWD", pwd);
    GRAB("AUTHMECH", authmech);
#undef GRAB
    v = setup_find_attr(attrs, "SSL");
    if (v) st->ssl = (*v == '1' || *v == 't' || *v == 'T');
    v = setup_find_attr(attrs, "SSLVerify");
    if (v) st->ssl_verify = !(*v == '0' || *v == 'f' || *v == 'F');
}

/* Persist the dialog result. */
static BOOL setup_save(const setup_state_t *st, LPCSTR driver)
{
    if (!SQLWriteDSNToIni(st->dsn, driver))
        return FALSE;
#define PUT(key, field)                                                      \
    do {                                                                     \
        if (!SQLWritePrivateProfileString(st->dsn, key, st->field,           \
                                          "ODBC.INI"))                       \
            return FALSE;                                                    \
    } while (0)
    PUT("BACKEND", backend);
    PUT("HOST", host);
    PUT("PORT", port);
    PUT("DATABASE", database);
    PUT("UID", uid);
    PUT("PWD", pwd);
#undef PUT
    SQLWritePrivateProfileString(st->dsn, "SSL", st->ssl ? "1" : "0",
                                 "ODBC.INI");
    SQLWritePrivateProfileString(st->dsn, "SSLVerify",
                                 st->ssl_verify ? "1" : "0", "ODBC.INI");
    SQLWritePrivateProfileString(st->dsn, "AUTHMECH",
                                 st->authmech[0] ? st->authmech : NULL,
                                 "ODBC.INI");
    return TRUE;
}

static BOOL setup_run_dialog(HWND parent, WORD fRequest, LPCSTR driver,
                             LPCSTR attrs)
{
    setup_state_t st;
    memset(&st, 0, sizeof(st));
    st.ssl_verify = TRUE;
    snprintf(st.backend, sizeof(st.backend), "hive");
    setup_load_attrs(&st, attrs);
    st.dsn_editable = (fRequest == ODBC_ADD_DSN);
    if (fRequest == ODBC_CONFIG_DSN && st.dsn[0])
        setup_load_dsn(&st);

    tmpl_t tmpl;
    int items = 0;
    const DLGTEMPLATE *dt = build_template(&tmpl, &items);

    INT_PTR rc = DialogBoxIndirectParamA(GetModuleHandleA(NULL), dt, parent,
                                         setup_dlgproc, (LPARAM)&st);
    if (rc != IDOK)
        return FALSE;   /* cancelled: not an installer error */

    BOOL ok = setup_save(&st, driver);
    SecureZeroMemory(st.pwd, sizeof(st.pwd));
    return ok;
}

/* ── ConfigDSN: create / configure / remove a data source ───────── */

ARGUS_SETUP_EXPORT BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD fRequest,
                                          LPCSTR lpszDriver,
                                          LPCSTR lpszAttributes)
{
    const char *dsn = setup_find_attr(lpszAttributes, "DSN");

    switch (fRequest) {
    case ODBC_ADD_DSN:
    case ODBC_CONFIG_DSN:
        /* Interactive request (odbcad32.exe): open the dialog whenever a
         * parent window exists — for Add even with a prefilled attribute
         * list, matching the commercial drivers' behaviour. */
        if (hwndParent)
            return setup_run_dialog(hwndParent, fRequest, lpszDriver,
                                    lpszAttributes);

        /* Scripted path: a usable DSN attribute is required. */
        if (!dsn || !*dsn || !SQLValidDSN(dsn)) {
            SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE,
                                  "No DSN= attribute; pass one (PowerShell "
                                  "Add-OdbcDsn, odbcconf) or run the setup "
                                  "interactively from odbcad32.exe.");
            return FALSE;
        }
        if (!SQLWriteDSNToIni(dsn, lpszDriver))
            return FALSE;
        return setup_write_attrs(dsn, lpszAttributes);

    case ODBC_REMOVE_DSN:
        if (!dsn || !*dsn || !SQLValidDSN(dsn)) {
            SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE,
                                  "ODBC_REMOVE_DSN needs a DSN= attribute.");
            return FALSE;
        }
        return SQLRemoveDSNFromIni(dsn);

    default:
        SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED,
                              "Unsupported ConfigDSN request");
        return FALSE;
    }
}

/* ── ConfigDriver: driver-level install/remove hook ─────────────── */

ARGUS_SETUP_EXPORT BOOL INSTAPI ConfigDriver(HWND hwndParent, WORD fRequest,
                                             LPCSTR lpszDriver,
                                             LPCSTR lpszArgs,
                                             LPSTR lpszMsg, WORD cbMsgMax,
                                             WORD *pcbMsgOut)
{
    (void)hwndParent;
    (void)lpszDriver;
    (void)lpszArgs;

    if (lpszMsg && cbMsgMax > 0)
        lpszMsg[0] = '\0';
    if (pcbMsgOut)
        *pcbMsgOut = 0;

    switch (fRequest) {
    case ODBC_INSTALL_DRIVER:
    case ODBC_REMOVE_DRIVER:
        /* No driver-specific post-install work is needed. */
        return TRUE;
    default:
        return FALSE;
    }
}

#endif /* _WIN32 */
