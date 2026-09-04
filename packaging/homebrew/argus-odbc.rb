# Homebrew formula for the Argus ODBC driver.
#
# Publish: create a tap repository (e.g. VargaFoundation/homebrew-argus) and
# copy this file to Formula/argus-odbc.rb. Users then run:
#   brew tap vargafoundation/argus && brew install argus-odbc
#
# On each release, bump `url` and recompute `sha256`:
#   shasum -a 256 <(curl -sL .../archive/refs/tags/vX.Y.Z.tar.gz)
class ArgusOdbc < Formula
  desc "Multi-backend ODBC driver for Hive, Impala, Trino, BigQuery and more"
  homepage "https://github.com/VargaFoundation/argus"
  url "https://github.com/VargaFoundation/argus/archive/refs/tags/v0.6.0.tar.gz"
  sha256 "e49fd2f72718591b377c3de858c3eb5d22ebf71c39412954fc9467c894923f6a"
  license "Apache-2.0"

  depends_on "cmake" => :build
  depends_on "pkg-config" => :build
  # The compiler only; Homebrew's thrift ships no c_glib runtime, so the
  # portable subset the driver needs is built from source below.
  depends_on "thrift" => :build
  depends_on "curl"
  depends_on "glib"
  depends_on "json-glib"
  depends_on "libpq"
  depends_on "mariadb-connector-c"
  depends_on "unixodbc"

  def install
    # libpq is keg-only, so its .pc file is not on the default search path;
    # without this the pkg-config probe misses it -- see docs/BUILDING.md.
    ENV.prepend_path "PKG_CONFIG_PATH", Formula["libpq"].opt_lib/"pkgconfig"
    ENV.prepend_path "PKG_CONFIG_PATH", Formula["mariadb-connector-c"].opt_lib/"pkgconfig"

    # Homebrew's thrift is the compiler and the C++ runtime only -- it does
    # not build c_glib -- so Hive and Impala used to be left out of every
    # bottle, which is the one thing most people install this driver for.
    # The portable subset is built here the way the CI macOS job does it. It
    # is a static archive, so nothing of it survives into a runtime path:
    # the driver links it in and the build prefix goes away with buildpath.
    thrift_prefix = buildpath/"thrift-c-glib-prefix"
    system "bash", "scripts/build-thrift-c-glib.sh", thrift_prefix, "0.23.0"
    ENV.prepend_path "PKG_CONFIG_PATH", thrift_prefix/"lib/pkgconfig"

    # Every backend this formula declares a dependency for is required, so a
    # probe that stops finding its library fails the build instead of
    # producing a bottle that quietly lacks the backend.
    system "cmake", "-B", "build",
           "-DCMAKE_BUILD_TYPE=Release",
           "-DBUILD_TESTING=OFF",
           "-DARGUS_WITH_HIVE=ON",
           "-DARGUS_WITH_IMPALA=ON",
           "-DARGUS_WITH_TRINO=ON",
           "-DARGUS_WITH_PHOENIX=ON",
           "-DARGUS_WITH_PINOT=ON",
           "-DARGUS_WITH_DRUID=ON",
           "-DARGUS_WITH_BIGQUERY=ON",
           "-DARGUS_WITH_MYSQL=ON",
           "-DARGUS_WITH_POSTGRES=ON",
           *std_cmake_args
    system "cmake", "--build", "build"
    lib.install "build/src/libargus_odbc.dylib"
    doc.install "README.md", "CONNECTION_EXAMPLES.md"
    (share/"argus").install "scripts/install_dsn_macos.sh"
  end

  def caveats
    <<~EOS
      Register the driver with unixODBC (once):
        odbcinst -i -d -n "Argus ODBC Driver" -f - <<INI
        [Argus ODBC Driver]
        Driver=#{opt_lib}/libargus_odbc.dylib
        INI
      Then create DSNs in ~/.odbc.ini, or run:
        #{share}/argus/install_dsn_macos.sh
    EOS
  end

  test do
    assert_predicate lib/"libargus_odbc.dylib", :exist?

    # The ODBC entry points must actually be exported, and the build manifest
    # the driver embeds must list the backends the keg-only dance above and
    # the -DARGUS_WITH_*=ON flags were meant to guarantee.
    symbols = shell_output("nm -gU #{lib}/libargus_odbc.dylib")
    assert_match "_SQLDriverConnect", symbols
    manifest = shell_output("strings -a #{lib}/libargus_odbc.dylib")[/^argus-build .*$/]
    refute_nil manifest, "no argus-build line in the driver"
    %w[hive impala trino phoenix pinot druid bigquery mysql postgres
       greenplum cloudberry].each do |backend|
      assert_match(/ #{backend}( |$)/, manifest)
    end
  end
end
