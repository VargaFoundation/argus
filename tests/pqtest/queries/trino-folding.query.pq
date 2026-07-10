// Folding test — the point-and-click + transform path (Navigator → filter → sort
// → top-N), routed through ArgusFold.pq which declares SqlCapabilities.
// LimitClauseKind. This is what the trino-folding known-issue said required a
// dedicated connector. The result is the top 3 names of region 1, alphabetical.
//
// Verifying it actually FOLDED (vs computed client-side) is done on Windows with
// "View Native Query" / Trino's system.runtime.queries — the result alone is the
// same either way, so this golden guards correctness of the folding path.
let
    conn = "driver={Argus ODBC Driver};BACKEND=trino;HOST=localhost;PORT=8080;DATABASE=tpch",
    tables = Argus.Feed(conn),
    nationRow = Table.SelectRows(tables, each [Item] = "nation" and [Schema] = "tiny"),
    nation = nationRow{0}[Data],
    cols = Table.SelectColumns(nation, {"name", "regionkey"}),
    filtered = Table.SelectRows(cols, each [regionkey] = 1),
    sorted = Table.Sort(filtered, {{"name", Order.Ascending}}),
    top3 = Table.FirstN(sorted, 3)
in
    top3
