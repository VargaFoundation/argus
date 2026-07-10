// Folding-capable Argus connector for PQTest — the fix for the trino-folding
// known-issue. Same as ArgusNav.pq (Odbc.DataSource → Navigator tree) but it
// declares SqlCapabilities.LimitClauseKind, which is what lets Power Query fold
// ORDER BY + top-N/LIMIT into SQL. Trino uses ANSI OFFSET…FETCH, so
// LimitClauseKind.AnsiSql2008. This mirrors the product connector in
// connectors/powerbi/Argus.pq.
section Argus;

[DataSource.Kind="Argus"]
shared Argus.Feed = (connectionString as text) as table =>
    Odbc.DataSource(connectionString, [
        HierarchicalNavigation = false,
        SqlCapabilities = [
            SupportsTop = false,
            LimitClauseKind = LimitClauseKind.AnsiSql2008
        ]
    ]);

Argus = [
    Authentication = [
        Anonymous = [],
        UsernamePassword = []
    ]
];
