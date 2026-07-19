// Builds the ODBC connection string Tableau hands to the Argus driver.
//
// Keywords on the right are the driver's own (src/odbc/dsn.c). BACKEND is
// pinned to hive because a .taco carries a single dialect; the same backend
// serves Spark Thrift Server and Flink SQL Gateway over HiveServer2.
(function dsbuilder(attr)
{
    var params = {};

    params["BACKEND"] = "hive";
    params["HOST"] = attr[connectionHelper.attributeServer];
    params["PORT"] = attr[connectionHelper.attributePort];

    var database = attr[connectionHelper.attributeDatabase];
    if (database) {
        params["DATABASE"] = database;
    }

    // The driver's AuthMech values, not Tableau's names.
    var authentication = attr[connectionHelper.attributeAuthentication];
    if (authentication == "auth-none") {
        params["AuthMech"] = "NOSASL";
    } else if (authentication == "auth-integrated") {
        params["AuthMech"] = "KERBEROS";

        var krbService = attr["krbservicename"];
        if (krbService) {
            params["KrbServiceName"] = krbService;
        }
        var krbHost = attr["krbhostfqdn"];
        if (krbHost) {
            params["KrbHostFQDN"] = krbHost;
        }
    } else {
        // SASL PLAIN, with or without a password (Hive accepts a bare user).
        params["AuthMech"] = "PLAIN";
        params["UID"] = attr[connectionHelper.attributeUsername];
        if (authentication == "auth-user-pass") {
            params["PWD"] = attr[connectionHelper.attributePassword];
        }
    }

    if (attr[connectionHelper.attributeSSLMode] == "require") {
        params["SSL"] = "1";
    }

    // Driver keywords the dialog does not expose (TransportMode=HTTP + HttpPath
    // for Knox, timeouts, OAuth2) without needing a new connector build.
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
