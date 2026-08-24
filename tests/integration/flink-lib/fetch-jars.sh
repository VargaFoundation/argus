#!/bin/sh
# Fetch the two jars the Flink SQL Gateway's hiveserver2 endpoint needs
# (mounted by docker-compose.yml). Run once; ~95 MB total, sha256-pinned.
set -eu
cd "$(dirname "$0")"
fetch() {
    f=$1; url=$2; sum=$3
    [ -f "$f" ] && echo "$sum  $f" | sha256sum -c - >/dev/null 2>&1 && return 0
    echo "fetching $f"
    curl -fsSL -o "$f" "$url"
    echo "$sum  $f" | sha256sum -c -
}
fetch flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.18.1/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar \
  0d518987c7fdb12f526b43500c75692927b89c439761f0e2c1ce7b45f68811e2
fetch flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
  492b2a559f2a1dad3808b51d9a26a575dbb1202004c9f85f5059c520e0632127
echo "flink-lib ready"
