# Connection String Examples

Quick reference for Argus ODBC Driver connection strings.

## Basic Connections

### Hive (Default)
```
HOST=hive-server;PORT=10000;UID=admin;PWD=secret;DATABASE=mydb
```

### Impala
```
HOST=impala-server;PORT=21050;UID=admin;PWD=secret;DATABASE=mydb;BACKEND=impala
```

### Trino
```
HOST=trino-server;PORT=8080;UID=admin;PWD=secret;DATABASE=hive;BACKEND=trino
```

## Production Configurations

### Hive with SSL and Logging
```
HOST=hive.example.com;PORT=10000;UID=admin;PWD=secret;
SSL=1;SSLCAFile=/etc/ssl/certs/ca-bundle.crt;SSLVerify=1;
LogLevel=4;LogFile=/var/log/argus-hive.log;
BACKEND=hive;ApplicationName=ETL-Pipeline
```

### Impala with Retry and Timeout
```
HOST=impala.example.com;PORT=21050;UID=admin;PWD=secret;
DATABASE=warehouse;BACKEND=impala;
RetryCount=3;RetryDelay=2;
ConnectTimeout=30;QueryTimeout=600;SocketTimeout=120;
FetchBufferSize=5000;ApplicationName=Analytics
```

### Trino with HTTPS
```
HOST=trino.example.com;PORT=8443;UID=admin;PWD=secret;
SSL=1;SSLCAFile=/etc/ssl/certs/trino-ca.pem;
SSLCertFile=/etc/ssl/certs/client-cert.pem;
SSLKeyFile=/etc/ssl/private/client-key.pem;
BACKEND=trino;LogLevel=5;ApplicationName=Dashboard
```

## Development/Testing

### Local Hive with Debug Logging
```
HOST=localhost;PORT=10000;UID=dev;PWD=dev;DATABASE=test_db;
LogLevel=6;LogFile=/tmp/argus-debug.log;BACKEND=hive
```

### Local Trino (HTTP)
```
HOST=localhost;PORT=8080;UID=admin;DATABASE=memory;
BACKEND=trino;LogLevel=5;LogFile=/tmp/trino-debug.log
```

### Impala with Minimal Config
```
HOST=localhost;PORT=21050;BACKEND=impala
```

## Docker/Kubernetes

### Hive in Docker
```
HOST=hive-metastore;PORT=10000;UID=hive;PWD=hive;
DATABASE=default;BACKEND=hive;ConnectTimeout=10;RetryCount=5
```

### Trino in Kubernetes
```
HOST=trino-service.default.svc.cluster.local;PORT=8080;
UID=admin;BACKEND=trino;
ConnectTimeout=15;QueryTimeout=300;ApplicationName=K8s-App
```

## Cloud Environments

### AWS EMR Hive
```
HOST=ec2-xx-xx-xx-xx.compute.amazonaws.com;PORT=10000;
UID=hadoop;PWD=hadoop;DATABASE=default;BACKEND=hive;
ConnectTimeout=20;RetryCount=3;RetryDelay=2;
ApplicationName=AWS-ETL;LogLevel=4
```

### GCP Dataproc
```
HOST=dataproc-cluster-m;PORT=10000;UID=hive;
DATABASE=warehouse;BACKEND=hive;
SocketTimeout=180;QueryTimeout=1800;FetchBufferSize=10000
```

## High-Security Environments

### Mutual TLS (Trino)
```
HOST=secure-trino.corp.com;PORT=8443;UID=service-account;
SSL=1;SSLVerify=1;
SSLCertFile=/opt/certs/client-cert.pem;
SSLKeyFile=/opt/certs/client-key.pem;
SSLCAFile=/opt/certs/root-ca.pem;
BACKEND=trino;LogLevel=3;ApplicationName=SecureService
```

### SSL with Self-Signed Cert (Testing)
```
HOST=test-hive;PORT=10000;UID=test;PWD=test;
SSL=1;SSLVerify=0;SSLCAFile=/tmp/self-signed-ca.pem;
BACKEND=hive;LogLevel=6;LogFile=/tmp/ssl-test.log
```

## Performance Tuning

### Large Result Sets
```
HOST=hive-server;PORT=10000;UID=analyst;PWD=analyst;
DATABASE=warehouse;FetchBufferSize=20000;
SocketTimeout=300;QueryTimeout=3600;BACKEND=hive
```

### Low Latency Queries
```
HOST=impala-server;PORT=21050;UID=app;DATABASE=metrics;
BACKEND=impala;FetchBufferSize=1000;
ConnectTimeout=5;QueryTimeout=60
```

### Connection Pooling Workload
```
HOST=hive-lb;PORT=10000;UID=pool-user;PWD=pool-pass;
RetryCount=5;RetryDelay=1;ConnectTimeout=10;
BACKEND=hive;ApplicationName=WebApp
```

## Environment-Specific

### Development
```
HOST=dev-hive;PORT=10000;UID=dev;PWD=dev;
LogLevel=6;LogFile=/tmp/dev-argus.log;BACKEND=hive
```

### Staging
```
HOST=staging-impala;PORT=21050;UID=staging;PWD=staging;
LogLevel=4;LogFile=/var/log/staging-argus.log;
RetryCount=3;BACKEND=impala;ApplicationName=Staging-Tests
```

### Production
```
HOST=prod-trino.example.com;PORT=8443;UID=prod-service;PWD=xxx;
SSL=1;SSLVerify=1;SSLCAFile=/etc/ssl/certs/prod-ca.pem;
LogLevel=3;LogFile=/var/log/prod-argus.log;
ConnectTimeout=30;QueryTimeout=900;RetryCount=3;
BACKEND=trino;ApplicationName=Production-Service
```

## DSN Configuration (odbcinst.ini)

### System DSN Example
```ini
[ArgusHive]
Driver=/usr/local/lib/libargus_odbc.so
Description=Argus ODBC Driver for Apache Hive
HOST=hive-server
PORT=10000
UID=hiveuser
DATABASE=default
BACKEND=hive
LogLevel=4
LogFile=/var/log/argus-hive.log
```

### User DSN Example
```ini
[MyTrino]
Driver=/home/user/lib/libargus_odbc.so
HOST=trino.example.com
PORT=8443
UID=myuser
SSL=1
BACKEND=trino
LogLevel=5
ApplicationName=MyApp
```

## Programmatic Connection (C/C++)

### Using SQLDriverConnect
```c
const char *conn_str = 
    "HOST=hive-server;PORT=10000;UID=admin;PWD=secret;"
    "DATABASE=mydb;BACKEND=hive;LogLevel=5";

SQLRETURN ret = SQLDriverConnect(
    dbc, NULL, 
    (SQLCHAR*)conn_str, SQL_NTS,
    NULL, 0, NULL, 
    SQL_DRIVER_NOPROMPT);
```

### Using SQLConnect with DSN
```c
SQLRETURN ret = SQLConnect(
    dbc,
    (SQLCHAR*)"ArgusHive", SQL_NTS,
    (SQLCHAR*)"admin", SQL_NTS,
    (SQLCHAR*)"secret", SQL_NTS);
```

## Parameter Priority

When same parameter specified multiple ways, priority is:
1. Connection string parameter (highest)
2. DSN configuration
3. Environment variable
4. Default value (lowest)

Example:
```bash
# Environment variable
export ARGUS_LOG_LEVEL=4

# DSN has LogLevel=5

# Connection string
"...;LogLevel=6"  # This wins (value=6)
```

## Tips

1. **Always quote file paths** with spaces
2. **Use absolute paths** for SSL certificates
3. **Enable logging** when troubleshooting: `LogLevel=6`
4. **Set timeouts** for production: `ConnectTimeout=30;QueryTimeout=600`
5. **Use retry logic** for flaky networks: `RetryCount=3`
6. **Name your apps** for query tracking: `ApplicationName=YourApp`
7. **Test SSL first** with `SSLVerify=0`, then enable verification
