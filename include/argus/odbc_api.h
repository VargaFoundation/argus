#ifndef ARGUS_ODBC_API_H
#define ARGUS_ODBC_API_H

#ifdef _WIN32
#include <windows.h>
#endif
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

/* Export macro for shared library symbols */
#if defined(_WIN32) || defined(__CYGWIN__)
    #ifdef ARGUS_BUILD_DLL
        #define ARGUS_EXPORT __declspec(dllexport)
    #else
        #define ARGUS_EXPORT __declspec(dllimport)
    #endif
#else
    #ifdef ARGUS_BUILD_DLL
        #define ARGUS_EXPORT __attribute__((visibility("default")))
    #else
        #define ARGUS_EXPORT
    #endif
#endif

/*
 * ODBC API function declarations.
 * These are the entry points exported by libargus_odbc.so.
 */

/* Handle management */
ARGUS_EXPORT SQLRETURN SQL_API SQLAllocHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   InputHandle,
    SQLHANDLE  *OutputHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLFreeHandle(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle);

ARGUS_EXPORT SQLRETURN SQL_API SQLFreeStmt(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT Option);

/* Connection */
ARGUS_EXPORT SQLRETURN SQL_API SQLConnect(
    SQLHDBC     ConnectionHandle,
    SQLCHAR    *ServerName, SQLSMALLINT NameLength1,
    SQLCHAR    *UserName,   SQLSMALLINT NameLength2,
    SQLCHAR    *Authentication, SQLSMALLINT NameLength3);

ARGUS_EXPORT SQLRETURN SQL_API SQLDriverConnect(
    SQLHDBC      ConnectionHandle,
    SQLHWND      WindowHandle,
    SQLCHAR     *InConnectionString, SQLSMALLINT StringLength1,
    SQLCHAR     *OutConnectionString, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr,
    SQLUSMALLINT DriverCompletion);

ARGUS_EXPORT SQLRETURN SQL_API SQLDisconnect(
    SQLHDBC ConnectionHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLBrowseConnect(
    SQLHDBC     ConnectionHandle,
    SQLCHAR    *InConnectionString, SQLSMALLINT StringLength1,
    SQLCHAR    *OutConnectionString, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr);

/* Query execution */
ARGUS_EXPORT SQLRETURN SQL_API SQLExecDirect(
    SQLHSTMT  StatementHandle,
    SQLCHAR  *StatementText,
    SQLINTEGER TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLPrepare(
    SQLHSTMT  StatementHandle,
    SQLCHAR  *StatementText,
    SQLINTEGER TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLExecute(
    SQLHSTMT StatementHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLNativeSql(
    SQLHDBC    ConnectionHandle,
    SQLCHAR   *InStatementText, SQLINTEGER TextLength1,
    SQLCHAR   *OutStatementText, SQLINTEGER BufferLength,
    SQLINTEGER *TextLength2Ptr);

ARGUS_EXPORT SQLRETURN SQL_API SQLCancel(
    SQLHSTMT StatementHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLRowCount(
    SQLHSTMT StatementHandle,
    SQLLEN  *RowCount);

ARGUS_EXPORT SQLRETURN SQL_API SQLMoreResults(
    SQLHSTMT StatementHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLParamData(
    SQLHSTMT  StatementHandle,
    SQLPOINTER *Value);

ARGUS_EXPORT SQLRETURN SQL_API SQLPutData(
    SQLHSTMT  StatementHandle,
    SQLPOINTER Data,
    SQLLEN     StrLen_or_Ind);

ARGUS_EXPORT SQLRETURN SQL_API SQLNumParams(
    SQLHSTMT     StatementHandle,
    SQLSMALLINT *ParameterCountPtr);

/* Result fetching */
ARGUS_EXPORT SQLRETURN SQL_API SQLFetch(
    SQLHSTMT StatementHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLFetchScroll(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT FetchOrientation,
    SQLLEN      FetchOffset);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetData(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT  TargetType,
    SQLPOINTER   TargetValue,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_Ind);

ARGUS_EXPORT SQLRETURN SQL_API SQLBindCol(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLSMALLINT  TargetType,
    SQLPOINTER   TargetValue,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_Ind);

ARGUS_EXPORT SQLRETURN SQL_API SQLNumResultCols(
    SQLHSTMT     StatementHandle,
    SQLSMALLINT *ColumnCount);

ARGUS_EXPORT SQLRETURN SQL_API SQLDescribeCol(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLCHAR     *ColumnName, SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr,
    SQLULEN     *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr,
    SQLSMALLINT *NullablePtr);

ARGUS_EXPORT SQLRETURN SQL_API SQLColAttribute(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER   CharacterAttribute,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN      *NumericAttribute);

ARGUS_EXPORT SQLRETURN SQL_API SQLCloseCursor(
    SQLHSTMT StatementHandle);

ARGUS_EXPORT SQLRETURN SQL_API SQLBindParameter(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT ParameterNumber,
    SQLSMALLINT  InputOutputType,
    SQLSMALLINT  ValueType,
    SQLSMALLINT  ParameterType,
    SQLULEN      ColumnSize,
    SQLSMALLINT  DecimalDigits,
    SQLPOINTER   ParameterValuePtr,
    SQLLEN       BufferLength,
    SQLLEN      *StrLen_or_IndPtr);

/* Catalog functions */
ARGUS_EXPORT SQLRETURN SQL_API SQLTables(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR    *TableType,   SQLSMALLINT NameLength4);

ARGUS_EXPORT SQLRETURN SQL_API SQLColumns(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR    *ColumnName,  SQLSMALLINT NameLength4);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetTypeInfo(
    SQLHSTMT    StatementHandle,
    SQLSMALLINT DataType);

ARGUS_EXPORT SQLRETURN SQL_API SQLStatistics(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Unique,
    SQLUSMALLINT Reserved);

ARGUS_EXPORT SQLRETURN SQL_API SQLSpecialColumns(
    SQLHSTMT    StatementHandle,
    SQLUSMALLINT IdentifierType,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3,
    SQLUSMALLINT Scope,
    SQLUSMALLINT Nullable);

ARGUS_EXPORT SQLRETURN SQL_API SQLPrimaryKeys(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3);

ARGUS_EXPORT SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *PKCatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *PKSchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *PKTableName,   SQLSMALLINT NameLength3,
    SQLCHAR    *FKCatalogName, SQLSMALLINT NameLength4,
    SQLCHAR    *FKSchemaName,  SQLSMALLINT NameLength5,
    SQLCHAR    *FKTableName,   SQLSMALLINT NameLength6);

ARGUS_EXPORT SQLRETURN SQL_API SQLProcedures(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *ProcName,    SQLSMALLINT NameLength3);

ARGUS_EXPORT SQLRETURN SQL_API SQLProcedureColumns(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *ProcName,    SQLSMALLINT NameLength3,
    SQLCHAR    *ColumnName,  SQLSMALLINT NameLength4);

ARGUS_EXPORT SQLRETURN SQL_API SQLTablePrivileges(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3);

ARGUS_EXPORT SQLRETURN SQL_API SQLColumnPrivileges(
    SQLHSTMT    StatementHandle,
    SQLCHAR    *CatalogName, SQLSMALLINT NameLength1,
    SQLCHAR    *SchemaName,  SQLSMALLINT NameLength2,
    SQLCHAR    *TableName,   SQLSMALLINT NameLength3,
    SQLCHAR    *ColumnName,  SQLSMALLINT NameLength4);

/* Information */
ARGUS_EXPORT SQLRETURN SQL_API SQLGetInfo(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT InfoType,
    SQLPOINTER   InfoValue,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetFunctions(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT FunctionId,
    SQLUSMALLINT *Supported);

/* Diagnostics */
ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagRec(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLCHAR    *Sqlstate,
    SQLINTEGER *NativeError,
    SQLCHAR    *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagField(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT DiagIdentifier,
    SQLPOINTER  DiagInfo,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLError(
    SQLHENV   EnvironmentHandle,
    SQLHDBC   ConnectionHandle,
    SQLHSTMT  StatementHandle,
    SQLCHAR  *Sqlstate,
    SQLINTEGER *NativeError,
    SQLCHAR  *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength);

/* Attributes */
ARGUS_EXPORT SQLRETURN SQL_API SQLSetEnvAttr(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetEnvAttr(
    SQLHENV    EnvironmentHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLSetConnectAttr(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetConnectAttr(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLSetStmtAttr(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetStmtAttr(
    SQLHSTMT   StatementHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength);

/* Miscellaneous */
ARGUS_EXPORT SQLRETURN SQL_API SQLEndTran(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT CompletionType);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetCursorName(
    SQLHSTMT   StatementHandle,
    SQLCHAR   *CursorName,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr);

ARGUS_EXPORT SQLRETURN SQL_API SQLSetCursorName(
    SQLHSTMT  StatementHandle,
    SQLCHAR  *CursorName,
    SQLSMALLINT NameLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLCopyDesc(
    SQLHDESC SourceDescHandle,
    SQLHDESC TargetDescHandle);

/* Descriptors */
ARGUS_EXPORT SQLRETURN SQL_API SQLGetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength,
    SQLINTEGER *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLSetDescField(
    SQLHDESC    DescriptorHandle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT FieldIdentifier,
    SQLPOINTER  Value,
    SQLINTEGER  BufferLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDescRec(
    SQLHDESC     DescriptorHandle,
    SQLSMALLINT  RecNumber,
    SQLCHAR     *Name,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLengthPtr,
    SQLSMALLINT *TypePtr,
    SQLSMALLINT *SubTypePtr,
    SQLLEN      *LengthPtr,
    SQLSMALLINT *PrecisionPtr,
    SQLSMALLINT *ScalePtr,
    SQLSMALLINT *NullablePtr);

/* ── Unicode (W) variants ────────────────────────────────────── */

ARGUS_EXPORT SQLRETURN SQL_API SQLDriverConnectW(
    SQLHDBC      ConnectionHandle,
    SQLHWND      WindowHandle,
    SQLWCHAR    *InConnectionString, SQLSMALLINT StringLength1,
    SQLWCHAR    *OutConnectionString, SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength2Ptr,
    SQLUSMALLINT DriverCompletion);

ARGUS_EXPORT SQLRETURN SQL_API SQLConnectW(
    SQLHDBC   ConnectionHandle,
    SQLWCHAR *ServerName,      SQLSMALLINT NameLength1,
    SQLWCHAR *UserName,        SQLSMALLINT NameLength2,
    SQLWCHAR *Authentication,  SQLSMALLINT NameLength3);

ARGUS_EXPORT SQLRETURN SQL_API SQLExecDirectW(
    SQLHSTMT  StatementHandle,
    SQLWCHAR *StatementText,
    SQLINTEGER TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLPrepareW(
    SQLHSTMT  StatementHandle,
    SQLWCHAR *StatementText,
    SQLINTEGER TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagRecW(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLWCHAR   *Sqlstate,
    SQLINTEGER *NativeError,
    SQLWCHAR   *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetDiagFieldW(
    SQLSMALLINT HandleType,
    SQLHANDLE   Handle,
    SQLSMALLINT RecNumber,
    SQLSMALLINT DiagIdentifier,
    SQLPOINTER  DiagInfo,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLColAttributeW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLUSMALLINT FieldIdentifier,
    SQLPOINTER   CharacterAttribute,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength,
    SQLLEN      *NumericAttribute);

ARGUS_EXPORT SQLRETURN SQL_API SQLDescribeColW(
    SQLHSTMT     StatementHandle,
    SQLUSMALLINT ColumnNumber,
    SQLWCHAR    *ColumnName, SQLSMALLINT BufferLength,
    SQLSMALLINT *NameLengthPtr,
    SQLSMALLINT *DataTypePtr,
    SQLULEN     *ColumnSizePtr,
    SQLSMALLINT *DecimalDigitsPtr,
    SQLSMALLINT *NullablePtr);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetInfoW(
    SQLHDBC      ConnectionHandle,
    SQLUSMALLINT InfoType,
    SQLPOINTER   InfoValue,
    SQLSMALLINT  BufferLength,
    SQLSMALLINT *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLTablesW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName, SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,  SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,   SQLSMALLINT NameLength3,
    SQLWCHAR  *TableType,   SQLSMALLINT NameLength4);

ARGUS_EXPORT SQLRETURN SQL_API SQLColumnsW(
    SQLHSTMT   StatementHandle,
    SQLWCHAR  *CatalogName, SQLSMALLINT NameLength1,
    SQLWCHAR  *SchemaName,  SQLSMALLINT NameLength2,
    SQLWCHAR  *TableName,   SQLSMALLINT NameLength3,
    SQLWCHAR  *ColumnName,  SQLSMALLINT NameLength4);

ARGUS_EXPORT SQLRETURN SQL_API SQLNativeSqlW(
    SQLHDBC    ConnectionHandle,
    SQLWCHAR  *InStatementText,  SQLINTEGER TextLength1,
    SQLWCHAR  *OutStatementText, SQLINTEGER BufferLength,
    SQLINTEGER *TextLength2Ptr);

ARGUS_EXPORT SQLRETURN SQL_API SQLSetConnectAttrW(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLGetConnectAttrW(
    SQLHDBC    ConnectionHandle,
    SQLINTEGER Attribute,
    SQLPOINTER Value,
    SQLINTEGER BufferLength,
    SQLINTEGER *StringLength);

ARGUS_EXPORT SQLRETURN SQL_API SQLErrorW(
    SQLHENV   EnvironmentHandle,
    SQLHDBC   ConnectionHandle,
    SQLHSTMT  StatementHandle,
    SQLWCHAR *Sqlstate,
    SQLINTEGER *NativeError,
    SQLWCHAR *MessageText,
    SQLSMALLINT BufferLength,
    SQLSMALLINT *TextLength);

#endif /* ARGUS_ODBC_API_H */
