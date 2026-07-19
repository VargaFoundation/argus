// Builds the ODBC connection string Tableau hands to the Argus driver.
//
// Keywords on the right are the driver's own (src/odbc/dsn.c). No HOST or PORT:
// bigquery_connect ignores them, so sending them would only be noise.
(function dsbuilder(attr)
{
    var params = {};

    params["BACKEND"] = "bigquery";
    params["BQProject"] = attr["project"];

    var database = attr[connectionHelper.attributeDatabase];
    if (database) {
        params["DATABASE"] = database;
    }

    var location = attr["location"];
    if (location) {
        params["BQLocation"] = location;
    }

    if (attr[connectionHelper.attributeAuthentication] == "auth-access-token") {
        params["BQAccessToken"] = attr["accesstoken"];
    } else {
        params["BQKeyFile"] = attr["keyfile"];
    }

    // Sovereign cloud (S3NS) / emulator overrides; blank means public GCP.
    var endpoint = attr["endpoint"];
    if (endpoint) {
        params["BQEndpoint"] = endpoint;
    }
    var tokenEndpoint = attr["tokenendpoint"];
    if (tokenEndpoint) {
        params["BQTokenEndpoint"] = tokenEndpoint;
    }

    // Driver keywords the dialog does not expose (BQAudience, BQScope,
    // timeouts) without needing a new connector build.
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
