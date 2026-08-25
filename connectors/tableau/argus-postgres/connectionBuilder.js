// Builds the ODBC connection string Tableau hands to the Argus driver.
//
// Keywords on the right are the driver's own (src/odbc/dsn.c). BACKEND is
// pinned to postgres because a .taco carries a single dialect.
(function dsbuilder(attr)
{
    var params = {};

    params["BACKEND"] = "postgres";
    params["HOST"] = attr[connectionHelper.attributeServer];
    params["PORT"] = attr[connectionHelper.attributePort];

    var database = attr[connectionHelper.attributeDatabase];
    if (database) {
        params["DATABASE"] = database;
    }

    params["UID"] = attr[connectionHelper.attributeUsername];
    if (attr[connectionHelper.attributeAuthentication] == "auth-user-pass") {
        params["PWD"] = attr[connectionHelper.attributePassword];
    }

    // SSL=1 with the driver's default SSLVerify=1 is sslmode=verify-full.
    // Certificate paths stay out of the dialog — they are per-machine and go
    // in the DSN or in odbc-connect-string-extras below.
    if (attr[connectionHelper.attributeSSLMode] == "require") {
        params["SSL"] = "1";
    }

    // Driver keywords the dialog does not expose (timeouts, TLS certificates,
    // ARGUS_PG_SHOW_PARTITIONS' connection-string equivalent) without needing a
    // new connector build.
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
