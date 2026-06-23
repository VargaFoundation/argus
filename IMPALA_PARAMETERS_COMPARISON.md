# Comparaison des Paramètres Impala ODBC

Comparaison entre Argus ODBC et les drivers commerciaux (CData, Simba/Cloudera).

## ✅ Paramètres Implémentés

### Connexion de Base
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| HOST/Server | ✅ | ✅ | Hostname du serveur Impala |
| PORT | ✅ | ✅ | Port HiveServer2 (défaut: 21050 binary, 28000 HTTP) |
| UID/User | ✅ | ✅ | Nom d'utilisateur |
| PWD/Password | ✅ | ✅ | Mot de passe |
| DATABASE/Schema | ✅ | ✅ | Base de données par défaut |
| BACKEND | ✅ | N/A | Sélection backend (hive/impala/trino) |

### SSL/TLS
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| SSL/UseSSL | ✅ | ✅ | Active SSL/TLS |
| SSLVerify | ✅ | ❌ | Vérification certificat serveur |
| SSLCAFile/TrustedCerts | ✅ | ✅ | Certificat CA |
| SSLCertFile | ✅ | ✅ | Certificat client |
| SSLKeyFile | ✅ | ✅ | Clé privée client |

### Timeouts
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| ConnectTimeout | ✅ | ✅ | Timeout connexion (secondes) |
| QueryTimeout | ✅ | ✅ | Timeout exécution requête |
| SocketTimeout | ✅ | ✅ | Timeout lecture socket |

### Performance
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| FetchBufferSize | ✅ | ✅ | Lignes par fetch (défaut: 1000) |

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
| ApplicationName | ✅ | ❌ | Nom application (traçabilité) |

## ⚠️ Paramètres à Ajouter (si nécessaire)

### Authentication Avancée
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| AuthMech | ✅ (basique) | ✅ | Basse - NOSASL/PLAIN implémentés |
| KrbRealm | ❌ | ✅ | Moyenne - pour Kerberos |
| KrbHostFQDN/KrbFQDN | ❌ | ✅ | Moyenne - pour Kerberos |
| KrbServiceName | ❌ | ✅ | Moyenne - pour Kerberos |
| DelegationUID | ❌ | ✅ | Basse - delegation utilisateur |

### Transport
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| TransportMode | ✅ | ✅ | binary (21050) / HTTP (28000), HTTP nécessite libcurl |
| HTTPPath | ✅ | ✅ | Basse - déjà implémenté |
| ThriftTransport | ❌ | ✅ | Basse - sasl/nosasl |

### Protocol Version
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| ProtocolVersion | ✅ (V6) | ✅ | Très basse - V6 hardcodé (correct) |

### Configuration Serveur
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| DefaultStringColumnLength | ❌ | ✅ | Basse - gestion interne OK |

### Options Avancées
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| UseNativeQuery | ❌ | ✅ | Basse - queries déjà natives |
| AsyncExecPollInterval | ❌ | ✅ | Basse - pas d'async pour l'instant |

## 🎯 Recommandations

### Priorité HAUTE - Bloquant en entreprise
1. **Support Kerberos / GSSAPI** (KrbRealm, KrbHostFQDN, KrbServiceName)
   - **Obligatoire** sur la plupart des clusters CDP/CDH sécurisés. Sans Kerberos,
     Impala est inaccessible sur la majorité des déploiements d'entreprise.
   - Nécessite libkrb5-dev. Voir `docs/ROADMAP.md` (Phase 1).
2. **OAuth2 / JWT** (mode HTTP) pour les déploiements modernes.

### Priorité MOYENNE - Utiles pour certains cas

1. **TransportMode HTTP** (port 28000) — ✅ **implémenté**
   - binary (port 21050) reste le défaut ; HTTP est désormais supporté
     (`TransportMode=HTTP;HTTPPath=cliservice`), y compris Kerberos/SPNEGO via
     libcurl (`AuthMech=GSSAPI`), comme pour Hive.

### Priorité BASSE - Non nécessaires
- DefaultStringColumnLength: géré automatiquement
- UseNativeQuery: toutes les queries sont natives
- AsyncExecPollInterval: pas d'async pour l'instant
- ProtocolVersion: V6 est correct pour Impala

## 🔍 Différences Impala vs Hive

### Impala-Specific
1. **Protocol Version**: Impala utilise **V6** (Hive utilise V10)
   - ✅ Argus utilise V6 pour Impala (correct)
   - Code: `impala_session.c` ligne 100

2. **Default Ports**:
   - Binary: 21050 (vs 10000 pour Hive)
   - HTTP: 28000 (vs 10001 pour Hive)

3. **Database Switching**:
   - Impala n'accepte PAS `use:database` dans OpenSession config
   - ✅ Argus utilise `USE <db>` statement après connexion (correct)
   - Code: `impala_session.c` lignes 159-216

4. **Application Name**:
   - Impala ne supporte PAS `hive.query.source` comme Hive
   - ❌ Argus devrait utiliser un mécanisme différent pour Impala

## ✨ Avantages d'Argus vs Commercial

Notre driver a des fonctionnalités que CData/Simba n'ont PAS:

1. ✅ **Multi-backend** (Hive + Impala + Trino dans un seul driver)
2. ✅ **Retry automatique** avec délai configurable
3. ✅ **Application Name** pour traçabilité (Hive uniquement)
4. ✅ **SSLVerify** pour contrôle granulaire SSL
5. ✅ **Logging avancé** (7 niveaux vs 2-3 pour commercial)
6. ✅ **Open Source** (vs $500-2000/licence commercial)

## 📊 Conclusion

**Couverture**: large pour les paramètres de base, SSL, timeouts, résilience.
**Statut**: utilisable sur clusters Impala **non kerberisés** (binary/TCP). **PAS
production-ready pour la majorité des clusters d'entreprise**, qui imposent Kerberos.
**Manque bloquant**:
- **Kerberos/GSSAPI** — requis par la plupart des déploiements sécurisés.
- **OAuth2/JWT** — pour les passerelles et déploiements modernes.

Le code respecte correctement les différences de protocole (V6 vs V10) et le
database switching (USE vs config). Le **transport HTTP est implémenté** pour Impala
(session/query/fetch/catalogue + USE via HTTP), avec Kerberos/SPNEGO via libcurl,
sur le même modèle que Hive. Voir `docs/ROADMAP.md`.

## 📝 Sources

- [CData Impala ODBC Driver](https://www.cdata.com/drivers/impala/odbc/)
- [Cloudera ODBC Connector for Impala Install Guide](https://downloads.cloudera.com/connectors/impala_odbc_2.6.14.1016/Cloudera-ODBC-Connector-for-Impala-Install-Guide.pdf)
- [Simba Impala ODBC Driver Documentation](https://www.simba.com/products/Impala/doc/ODBC_InstallGuide/win/content/odbc/im/strings.htm)
- [Cloudera Documentation - Configuring Impala ODBC](https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_odbc.html)
