// Smoke tests for the Argus Power BI connector.
// Run with the Power Query SDK ("Evaluate current file") against a live server.
// These exercise the folding paths (filter, group by, top-N) the connector adds.
let
    // Point at a reachable server. Trino's built-in tpch catalog needs no setup.
    db = Argus.Database("localhost", 8080, "trino", "tpch"),

    // Navigate to tpch.tiny.nation via the hierarchical Navigator.
    nation = db{[Name = "tiny", Kind = "Schema"]}[Data]{[Name = "nation", Kind = "Table"]}[Data],

    // Each of these should fold to SQL (check with Table.View / query diagnostics):
    filtered = Table.SelectRows(nation, each [nationkey] < 5),          // WHERE
    grouped  = Table.Group(nation, {"regionkey"}, {{"n", each Table.RowCount(_), Int64.Type}}), // GROUP BY
    topN     = Table.FirstN(Table.Sort(nation, {"name"}), 3),          // ORDER BY + LIMIT/FETCH

    results = [
        FilterRows = Table.RowCount(filtered),
        GroupRows  = Table.RowCount(grouped),
        TopNames   = topN[name]
    ]
in
    results;
