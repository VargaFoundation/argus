// Navigateur Power BI : Odbc.DataSource doit exposer la liste des tables.
// On verifie que l'arbre n'est pas vide (tpch expose 80 objets : 8 tables x
// (information_schema + tiny + ...)). Regression contre un Navigateur vide.
let
    conn = "driver={Argus ODBC Driver};BACKEND=trino;HOST=localhost;PORT=8080;DATABASE=tpch",
    tables = Argus.Feed(conn)
in
    Table.RowCount(tables)
