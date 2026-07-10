let
    conn = "driver={Argus ODBC Driver};BACKEND=trino;HOST=localhost;PORT=8080;DATABASE=tpch"
in
    Argus.Query(conn, "SELECT name, regionkey FROM tpch.tiny.nation WHERE regionkey = 1 ORDER BY name")
