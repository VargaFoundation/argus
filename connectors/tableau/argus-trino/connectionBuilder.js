// Builds the ODBC connection string Tableau hands to the Argus driver.
//
// The keywords on the right are the driver's own (see src/odbc/dsn.c); BACKEND
// is pinned to trino because a .taco carries a single dialect, so this
// connector only ever talks to Trino.
(function dsbuilder(attr)
{
    var params = {};

    params["BACKEND"] = "trino";
    params["HOST"] = attr[connectionHelper.attributeServer];
    params["PORT"] = attr[connectionHelper.attributePort];

    var database = attr[connectionHelper.attributeDatabase];
    if (database) {
        params["DATABASE"] = database;
    }

    var authentication = attr[connectionHelper.attributeAuthentication];
    if (authentication == "auth-user" || authentication == "auth-user-pass") {
        params["UID"] = attr[connectionHelper.attributeUsername];
    }
    if (authentication == "auth-user-pass") {
        params["PWD"] = attr[connectionHelper.attributePassword];
    }
    if (authentication == "auth-none") {
        params["AuthMech"] = "NOSASL";
    }

    if (attr[connectionHelper.attributeSSLMode] == "require") {
        params["SSL"] = "1";
    }

    // Let users pass driver keywords the dialog does not expose (Kerberos SPN
    // overrides, OAuth2, timeouts) without needing a new connector build.
    var odbcConnectStringExtrasMap = {};
    const attributeODBCConnectStringExtras = "odbc-connect-string-extras";
    if (attributeODBCConnectStringExtras in attr)
    {
        odbcConnectStringExtrasMap = connectionHelper.ParseODBCConnectString(
            attr[attributeODBCConnectStringExtras]);
    }
    for (var key in odbcConnectStringExtrasMap)
    {
        params[key] = odbcConnectStringExtrasMap[key];
    }

    var formattedParams = [];
    formattedParams.push(connectionHelper.formatKeyValuePair(
        driverLocator.keywordDriver, driverLocator.locateDriver(attr)));

    for (var key in params)
    {
        formattedParams.push(connectionHelper.formatKeyValuePair(key, params[key]));
    }

    return formattedParams;
})
