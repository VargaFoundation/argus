let
    conn = "driver={Argus ODBC Driver};BACKEND=trino;HOST=localhost;PORT=8080;DATABASE=tpch"
in
    Argus.Query(conn, "SELECT count(*) AS c FROM tpch.tiny.nation")
