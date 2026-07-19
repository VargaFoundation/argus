// Argus ODBC — Power BI custom connector.
//
// This is a thin Power Query (M) layer ON TOP OF the Argus ODBC driver: it does
// not talk to any backend itself, it drives the installed Argus ODBC driver via
// Odbc.DataSource and, crucially, tells Power Query how to fold operations
// (filter, group by, top-N/limit) into SQL for each backend's dialect. Without a
// connector, generic ODBC in Power BI cannot fold LIMIT/TOP; with it, Import and
// DirectQuery both fold.
//
// Build: Power Query SDK (VS Code extension) or MakePQX → Argus.mez.
// The Argus ODBC driver must be installed separately (the .mez only references it
// by driver name).
[Version = "1.0.0"]
section Argus;

// Driver name as registered by the Argus installer / odbcinst.
DriverName = "Argus ODBC Driver";

// Backends whose dialect uses ANSI OFFSET…FETCH for top-N; everything else uses
// LIMIT … OFFSET. This is the single most important folding decision.
//
// flightsql is deliberately absent even though it is a judgement call: the
// backend is multi-engine (Dremio, Doris, InfluxDB 3), so no single answer is
// right for all of them. LIMIT…OFFSET is what Dremio and Doris accept, which
// covers the common case; an InfluxDB 3 user who needs OFFSET…FETCH has to edit
// this list. Splitting flightsql into per-engine kinds needs the driver to
// report which engine it actually reached.
AnsiOffsetBackends = {"trino"};

BackendList = {
    "hive", "impala", "trino", "phoenix", "pinot",
    "druid", "mysql", "bigquery", "flightsql", "kudu"
};

[DataSource.Kind = "Argus", Publish = "Argus.Publish"]
shared Argus.Database = Value.ReplaceType(ArgusImpl, ArgusType);

ArgusType = type function (
    Host as (type text meta [
        Documentation.FieldCaption = "Server",
        Documentation.SampleValues = {"trino.example.com"}
    ]),
    Port as (type number meta [
        Documentation.FieldCaption = "Port",
        Documentation.SampleValues = {8080}
    ]),
    Backend as (type text meta [
        Documentation.FieldCaption = "Backend",
        Documentation.AllowedValues = BackendList
    ]),
    optional Database as (type text meta [
        Documentation.FieldCaption = "Database / catalog",
        Documentation.SampleValues = {"default"}
    ]),
    // Without this there is no way to reach a TLS endpoint from Get Data at
    // all, even though the driver has supported SSL all along. Certificate
    // paths (SSLCertFile / SSLCAFile / SSLVerify, see docs/CONFIGURATION.md)
    // stay out of the dialog: they are per-machine and belong in the DSN.
    optional UseSSL as (type logical meta [
        Documentation.FieldCaption = "Use SSL/TLS",
        Documentation.SampleValues = {true}
    ])
) as table meta [
    Documentation.Name = "Argus",
    Documentation.LongDescription =
        "Connects to a data warehouse (Trino, Hive, Impala, StarRocks/Doris, "
        & "BigQuery, Pinot, Druid, Phoenix, …) through the Argus ODBC driver."
];

ArgusImpl = (Host as text, Port as number, Backend as text,
             optional Database as text, optional UseSSL as logical) as table =>
    let
        db = if Database <> null and Database <> "" then Database else "default",

        // Non-secret parts of the connection string. Credentials (UID/PWD) are
        // supplied by Power BI through CredentialConnectionString, never here.
        BaseConnectionString = [
            DRIVER   = DriverName,
            HOST     = Host,
            PORT     = Number.ToText(Port),
            BACKEND  = Backend,
            DATABASE = db
        ],

        // The driver reads SSL=1 (src/odbc/dsn.c). Only add the key when TLS is
        // wanted, so an unset checkbox leaves the string exactly as before.
        ConnectionString =
            if UseSSL = true
            then Record.AddField(BaseConnectionString, "SSL", "1")
            else BaseConnectionString,

        Credential = Extension.CurrentCredential(),
        // Map Power BI's auth kinds onto the driver's connection parameters.
        CredentialConnectionString =
            if Credential[AuthenticationKind]? = "UsernamePassword" then
                [ UID = Credential[Username], PWD = Credential[Password] ]
            else if Credential[AuthenticationKind]? = "Windows" then
                // Kerberos/SSPI on Windows uses the logged-in domain identity.
                [ AuthMech = "KERBEROS" ]
            else
                [ AuthMech = "NOSASL" ],

        LimitKind =
            if List.Contains(AnsiOffsetBackends, Backend)
            then LimitClauseKind.AnsiSql2008    // OFFSET m ROWS FETCH NEXT n ROWS ONLY
            else LimitClauseKind.LimitOffset,   // LIMIT n OFFSET m

        Options = [
            // Let Power BI push credentials into the DSN-less connection.
            CredentialConnectionString = CredentialConnectionString,

            // Show catalogs/schemas/tables as a hierarchy in the Navigator.
            HierarchicalNavigation = true,

            // The folding contract. LimitClauseKind is the essential bit: it
            // tells Power Query how to emit top-N (Trino → OFFSET…FETCH; others →
            // LIMIT…OFFSET), which generic ODBC can never fold on its own. The
            // driver already reports correct predicate/aggregate/GROUP BY
            // capabilities via SQLGetInfo, so we only pin what folding needs.
            SqlCapabilities = [
                SupportsTop = false,               // use LIMIT/OFFSET, not TOP n
                LimitClauseKind = LimitKind,
                // GROUP BY may reference any SELECT column (SQL_GB_GROUP_BY_CONTAINS_SELECT).
                GroupByCapabilities = 2,
                FractionalSecondsScale = 3,
                SupportsNumericLiterals = true,
                SupportsStringLiterals = true,
                SupportsOdbcDateLiterals = true,
                SupportsOdbcTimeLiterals = true,
                SupportsOdbcTimestampLiterals = true
            ],

            // Warehouses generally have no server-side prepared-parameter
            // binding; force literals so filters fold to SQL instead of being
            // evaluated client-side. (SQL_API_SQLBINDPARAMETER = 1002.)
            SQLGetFunctions = [
                SQL_API_SQLBINDPARAMETER = false
            ]
        ],

        Source = Odbc.DataSource(ConnectionString, Options)
    in
        Source;

// ── Data source kind ────────────────────────────────────────────
Argus = [
    // Identify a distinct data source by server + backend (not by credentials).
    // Must pass every parameter Argus.Database takes: the service replays this
    // for scheduled refresh, so an argument dropped here silently reverts to
    // its default — UseSSL omitted would refresh in cleartext.
    TestConnection = (dataSourcePath) =>
        let
            json = Json.Document(dataSourcePath),
            Host = json[Host],
            Port = json[Port],
            Backend = json[Backend],
            Database = json[Database]?,
            UseSSL = json[UseSSL]?
        in
            { "Argus.Database", Host, Port, Backend, Database, UseSSL },

    Authentication = [
        // Trino/Presto without auth, and other open backends.
        Anonymous = [],
        // Username/password (Trino LDAP, StarRocks/Doris/ClickHouse, Hive PLAIN).
        UsernamePassword = [],
        // Kerberos via native SSPI on a domain-joined Windows host.
        Windows = []
    ],

    Label = Extension.LoadString("DataSourceLabel")
];

// ── Publish to the Power BI "Get Data" UI ───────────────────────
Argus.Publish = [
    Beta = true,
    Category = "Database",
    ButtonText = { Extension.LoadString("ButtonTitle"), Extension.LoadString("ButtonHelp") },
    LearnMoreUrl = "https://github.com/VargaFoundation/argus",
    SourceImage = Argus.Icons,
    SourceTypeImage = Argus.Icons
];

Argus.Icons = [
    Icon16 = { Extension.Contents("Argus16.png"), Extension.Contents("Argus20.png"),
               Extension.Contents("Argus24.png"), Extension.Contents("Argus32.png") },
    Icon32 = { Extension.Contents("Argus32.png"), Extension.Contents("Argus40.png"),
               Extension.Contents("Argus48.png"), Extension.Contents("Argus64.png") }
];
