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
  depends_on "curl"
  depends_on "glib"
  depends_on "json-glib"
  depends_on "libpq"
  depends_on "mariadb-connector-c"
  depends_on "unixodbc"

  def install
    # libpq is keg-only, so its .pc file is not on the default search path.
    # Without this the PostgreSQL, Greenplum and Cloudberry backends are left
    # out of the build without any error -- see README.md.
    ENV.prepend_path "PKG_CONFIG_PATH", Formula["libpq"].opt_lib/"pkgconfig"

    system "cmake", "-B", "build",
           "-DCMAKE_BUILD_TYPE=Release",
           "-DBUILD_TESTING=OFF",
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

    # The ODBC entry points must actually be exported, and the PostgreSQL
    # backend must have survived the keg-only libpq dance above.
    symbols = shell_output("nm -gU #{lib}/libargus_odbc.dylib")
    assert_match "_SQLDriverConnect", symbols
    assert_match "postgres", shell_output("strings #{lib}/libargus_odbc.dylib")
  end
end
