# Comparaison des Paramètres Trino ODBC

Comparaison entre Argus ODBC et les drivers commerciaux (CData Presto, Simba Trino).

**Note**: CData propose un driver Presto ODBC qui fonctionne avec Trino (Trino = PrestoSQL fork).

## ✅ Paramètres Implémentés

### Connexion de Base
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| HOST/Server | ✅ | ✅ | Hostname du serveur Trino |
| PORT | ✅ | ✅ | Port HTTP (défaut: 8080, HTTPS: 8443/7778) |
| UID/User | ✅ | ✅ | Nom d'utilisateur |
| PWD/Password | ✅ | ✅ | Mot de passe |
| DATABASE/Schema | ✅ | ✅ | Schema par défaut |
| Catalog | ✅ | ✅ | Catalog Trino (requis) |
| BACKEND | ✅ | N/A | Sélection backend (hive/impala/trino) |

### SSL/TLS
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| SSL/UseSSL | ✅ | ✅ | Active SSL/TLS (HTTPS) |
| SSLVerify | ✅ | ❌ | Vérification certificat serveur |
| SSLCAFile/TrustedCerts | ✅ | ✅ | Certificat CA |
| SSLCertFile | ✅ | ✅ | Certificat client (mutual TLS) |
| SSLKeyFile | ✅ | ✅ | Clé privée client |
| AllowSelfSigned | ❌ | ✅ | Accepter certificats auto-signés |
| AllowHostNameCNMismatch | ❌ | ✅ | Ignorer hostname mismatch |

### Timeouts
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| ConnectTimeout | ✅ | ✅ | Timeout connexion (secondes) |
| QueryTimeout/Timeout | ✅ | ✅ | Timeout exécution requête |
| SocketTimeout | ✅ | ✅ | Timeout lecture socket |

### Performance
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| FetchBufferSize/MaxRows | ✅ | ✅ | Lignes par fetch (défaut: 1000) |

### Logging & Debug
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| LogLevel | ✅ | ✅ | Niveau de log (0-6) |
| LogFile | ✅ | ✅ | Fichier de log |

### Résilience
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| RetryCount | ✅ | ❌ | Nombre de tentatives |
| RetryDelay | ✅ | ❌ | Délai entre tentatives |

### Application
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| ApplicationName | ✅ | ✅ | Nom application (X-Trino-Source header) |

## ⚠️ Paramètres à Ajouter (si nécessaire)

### Authentication Avancée
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|----------|
| Basic / LDAP (PWD) | ✅ | ✅ | HTTP Basic (`AuthMech=BASIC/LDAP`, ou PWD seul) ; requiert TLS |
| JWT / OAuth2 Bearer | ✅ | ✅ | `AuthMech=JWT`, token dans PWD → `Authorization: Bearer` |
| Kerberos / SPNEGO | ✅ | ✅ | `AuthMech=GSSAPI`, SPNEGO via libcurl + ticket kinit |
| OAuth2 auth-code (SSO navigateur) | ❌ | ✅ | flux interactif token-endpoint non implémenté |

### SSL Avancé
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| AllowSelfSigned | ❌ | ✅ | Basse - SSLVerify=0 équivalent |
| AllowHostNameCNMismatch | ❌ | ✅ | Basse - géré par SSLVerify=0 |

### Session & Advanced
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| SessionProperties | ❌ | ✅ | Basse - propriétés session Trino |
| HTTPPath | ✅ | ✅ | Basse - déjà implémenté |

## 🎯 Recommandations

### Implémenté ✅
1. **HTTP Basic / LDAP** (`AuthMech=BASIC/LDAP/PLAIN`, ou PWD seul) — auth par
   mot de passe sur TLS. Validé.
2. **JWT / OAuth2 Bearer** (`AuthMech=JWT`, token dans PWD) → en-tête
   `Authorization: Bearer`. Validé.
3. **Kerberos / SPNEGO** (`AuthMech=GSSAPI`) via libcurl Negotiate + ticket kinit
   (même mécanisme que Hive/Impala HTTP, prouvé contre le cluster réel).
4. **OAuth2 client-credentials (M2M)** (`AuthMech=OAUTH2` + `OAuth2TokenEndpoint`/
   `ClientId`/`ClientSecret`/`Scope`) : Argus récupère un token au token-endpoint
   (grant_type=client_credentials) et l'utilise en Bearer. Validé bout-en-bout
   (mock IdP + mock Trino).

### Priorité MOYENNE - reste à faire
1. **OAuth2 flux interactifs** : auth-code + SSO navigateur (loopback/PKCE),
   device-code (headless), OIDC discovery, refresh automatique sur expiration.
   Ces flux nécessitent une interaction navigateur/utilisateur (peu courant pour
   un driver ODBC). Voir `docs/ROADMAP.md`.

### Priorité BASSE - Non nécessaires

1. **AllowSelfSigned / AllowHostNameCNMismatch**
   - Argus a déjà SSLVerify=0 qui désactive toute vérification
   - Fonctionnellement équivalent

2. **SessionProperties**
   - Permet de passer des propriétés Trino arbitraires
   - Cas d'usage niche (tuning avancé)

## 🔍 Spécificités Trino

### Architecture HTTP vs Thrift

**Trino utilise HTTP/REST** (pas Thrift comme Hive/Impala):
- ✅ Argus utilise libcurl pour HTTP/HTTPS (correct)
- ✅ Ports: 8080 (HTTP), 8443 (HTTPS), 7778 (HTTPS sécurisé)
- ✅ Headers: X-Trino-User, X-Trino-Source (ApplicationName)

### Catalog & Schema

**Trino requiert Catalog + Schema**:
- ✅ Argus supporte catalog via DATABASE parameter
- Exemple: `DATABASE=hive.default` (catalog.schema)
- Ou: `DATABASE=hive` (catalog seul)

### Authentication

**Trino supporte**:
1. No authentication (HTTP basic)
2. Basic Auth (user/password)
3. LDAP
4. Kerberos
5. JWT/OAuth2

✅ Argus implémente Basic Auth (cas le plus courant)

### SSL/TLS

**Trino utilise HTTPS standard**:
- ✅ Argus configure curl avec CURLOPT_SSL_VERIFYPEER
- ✅ Support certificats CA (CURLOPT_CAINFO)
- ✅ Support mutual TLS (CURLOPT_SSLCERT, CURLOPT_SSLKEY)

Code: `trino_session.c` lignes 28-47

### Application Name

**Trino utilise X-Trino-Source header**:
- ✅ Argus envoie ApplicationName dans X-Trino-Source
- Visible dans Trino query logs

Code: `trino_query.c` ligne 68

## ✨ Avantages d'Argus vs Commercial

Notre driver a des fonctionnalités que CData/Simba n'ont PAS:

1. ✅ **Multi-backend** (Hive + Impala + Trino dans un seul driver)
2. ✅ **Retry automatique** avec délai configurable
3. ✅ **SSLVerify granulaire** (vs AllowSelfSigned + AllowHostNameCNMismatch)
4. ✅ **Logging avancé** (7 niveaux vs 2-3 pour commercial)
5. ✅ **Open Source** (vs $500-2000/licence commercial)

## 📊 Conclusion

**Couverture**: large pour les paramètres de connexion de base, SSL/TLS (y compris
mTLS), timeouts, résilience et application name.
**Statut**: utilisable pour Trino sans SSO/OAuth (Basic auth, mTLS). **PAS encore au
niveau des drivers commerciaux pour la BI cloud**, qui exige OAuth2/OIDC.
**Manque bloquant**:
- **OAuth2 / OIDC + JWT** — standard de fait pour la BI cloud et les déploiements
  Trino modernes ; principal différenciateur des drivers commerciaux.
- **Kerberos / SPNEGO** — pour les déploiements adossés à un domaine AD.

L'architecture HTTP/REST est correctement implémentée avec libcurl, ce qui rend
l'ajout d'OAuth2/JWT plus accessible que sur le chemin Thrift. Voir `docs/ROADMAP.md`.

## 📝 Comparaison avec Implémentation Argus

### Code vérifié

1. **HTTP Transport** ✅
   - `trino_session.c`: utilise libcurl
   - HTTPS activé si ssl_enabled=true
   - Ports configurables

2. **Authentication** ✅
   - `trino_session.c`: Basic Auth (username:password)
   - Header X-Trino-User

3. **SSL/TLS** ✅
   - `trino_session.c`: configure CURLOPT_SSL_*
   - Support CA, client cert, verify

4. **Timeouts** ✅
   - ConnectTimeout: CURLOPT_CONNECTTIMEOUT
   - QueryTimeout: géré au niveau fetch

5. **Application Name** ✅
   - `trino_query.c`: header X-Trino-Source

6. **Catalog/Schema** ✅
   - `trino_connect()`: supporte catalog.schema

## 📝 Sources

- [CData Presto ODBC Driver](https://www.cdata.com/drivers/presto/odbc/)
- [CData Presto JDBC Driver - Connection String Options](http://cdn.cdata.com/help/ORF/jdbc/Connection.htm)
- [CData Presto JDBC Driver - Timeout](https://cdn.cdata.com/help/ORF/jdbc/RSBPresto_p_Timeout.htm)
- [Simba Trino ODBC Driver](https://insightsoftware.com/drivers/trino-odbc-jdbc/)
- [Simba Trino ODBC - Catalog and Schema Support](https://documentation.insightsoftware.com/trino-online-documentation-linux/content/odbc/features/catalog.htm)
- [Simba Trino ODBC - SSL Configuration](https://documentation.insightsoftware.com/trino-online-documentation-windows/content/odbc/configuring/ssl.htm)
- [Simba Trino ODBC - Security and Authentication](https://documentation.insightsoftware.com/trino-online-documentation-windows/content/odbc/features/security.htm)
- [Trino JDBC Documentation](https://trino.io/docs/current/client/jdbc.html)
