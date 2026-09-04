Name:           argus-odbc
Version:        %{version}
Release:        1%{?dist}
Summary:        Argus ODBC Driver for Data Warehouses
License:        Apache-2.0
URL:            https://github.com/VargaFoundation/argus

Source0:        argus-odbc-%{version}.tar.gz

Requires:       unixODBC
Requires:       glib2
Requires:       libcurl
Requires:       json-glib

%description
Argus is a universal ODBC driver for analytics engines: Hive, Impala,
Trino, Phoenix, Pinot, Druid, BigQuery, MySQL-wire (StarRocks/Doris/
ClickHouse), Arrow Flight SQL (Dremio/InfluxDB 3) and Kudu.

%prep
%setup -q -n argus-odbc-%{version}

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}%{_libdir}
mkdir -p %{buildroot}%{_includedir}/argus
cp lib/libargus_odbc.so %{buildroot}%{_libdir}/
cp include/argus/*.h %{buildroot}%{_includedir}/argus/

%post
if command -v odbcinst >/dev/null 2>&1; then
    odbcinst -i -d -n "Argus" -f /dev/stdin <<EOF
[Argus]
Description = Argus ODBC Driver for Data Warehouses
Driver = %{_libdir}/libargus_odbc.so
Setup = %{_libdir}/libargus_odbc.so
EOF
fi
ldconfig

%preun
# $1 is the number of this package that will remain: 0 on an erase, 1 on an
# upgrade. RPM runs the OLD %preun after the NEW %post, so without this test
# an upgrade unregistered the driver the new package had just registered,
# and the ODBC driver manager stopped finding it.
if [ "$1" = 0 ]; then
    if command -v odbcinst >/dev/null 2>&1; then
        odbcinst -u -d -n "Argus" 2>/dev/null || true
    fi
fi

%postun
ldconfig

%files
%license LICENSE
%doc NOTICE
%{_libdir}/libargus_odbc.so
%{_includedir}/argus/
