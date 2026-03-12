Name:           argus-odbc
Version:        %{version}
Release:        1%{?dist}
Summary:        Argus ODBC Driver for Data Warehouses
License:        Proprietary
URL:            https://github.com/varga/argus

Source0:        argus-odbc-%{version}.tar.gz

Requires:       unixODBC
Requires:       glib2
Requires:       libcurl
Requires:       json-glib

%description
Argus is a universal ODBC driver supporting Hive, Impala,
Trino, Phoenix, and Kudu backends.

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
if command -v odbcinst >/dev/null 2>&1; then
    odbcinst -u -d -n "Argus" 2>/dev/null || true
fi

%postun
ldconfig

%files
%{_libdir}/libargus_odbc.so
%{_includedir}/argus/
