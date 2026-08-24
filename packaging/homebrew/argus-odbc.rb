# Homebrew formula for the Argus ODBC driver.
#
# Publish: create a tap repository (e.g. VargaFoundation/homebrew-argus),
# copy this file to Formula/argus-odbc.rb, and fill url/sha256 from the first
# tagged release (`shasum -a 256 argus-odbc-<v>.tar.gz`). Users then run:
#   brew tap vargafoundation/argus && brew install argus-odbc
class ArgusOdbc < Formula
  desc "Multi-backend ODBC driver for Hive, Impala, Trino, BigQuery and more"
  homepage "https://github.com/VargaFoundation/argus"
  # TODO(release): point at the first tagged release tarball + its sha256.
  url "https://github.com/VargaFoundation/argus/archive/refs/tags/v0.6.0.tar.gz"
  sha256 "REPLACE_WITH_RELEASE_TARBALL_SHA256"
  license "Apache-2.0"

  depends_on "cmake" => :build
  depends_on "pkg-config" => :build
  depends_on "glib"
  depends_on "json-glib"
  depends_on "curl"
  depends_on "unixodbc"
  depends_on "mariadb-connector-c" => :optional

  def install
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
  end
end
