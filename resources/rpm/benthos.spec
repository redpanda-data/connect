Name:      benthos
Version:   %{_version}
Release:   %{_release}%{?dist}
Summary:   Persistent stream buffer service.

Group:     Applications/Services
License:   MIT
URL:       https://github.com/jeffail/benthos

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-%(%{__id_u} -n)

%description
This package provides a statically compiled Benthos service.

%build

%install
rm -rf $RPM_BUILD_ROOT
(cd %{_current_directory} && make DESTDIR=$RPM_BUILD_ROOT)

%clean
rm -rf $RPM_BUILD_ROOT

%files
%attr(-,root,root) %{_binpath}
