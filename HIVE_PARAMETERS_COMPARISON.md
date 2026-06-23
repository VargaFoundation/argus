# Comparaison des Paramètres Hive ODBC

Comparaison entre Argus ODBC et les drivers commerciaux (CData, Simba/Cloudera).

## ✅ Paramètres Implémentés

### Connexion de Base
| Paramètre | Argus | CData/Simba | Notes |
|-----------|-------|-------------|-------|
| HOST/Server | ✅ | ✅ | Hostname du serveur Hive |
| PORT | ✅ | ✅ | Port HiveServer2 (défaut: 10000) |
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
| KrbHostFQDN | ❌ | ✅ | Moyenne - pour Kerberos |
| KrbServiceName | ❌ | ✅ | Moyenne - pour Kerberos |
| DelegationUID | ❌ | ✅ | Basse - delegation utilisateur |

### Transport
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| TransportMode | ✅ | ✅ | binary/HTTP (Apache Knox, proxies) — HTTP nécessite un build avec libcurl |
| HTTPPath | ✅ | ✅ | Basse - déjà implémenté |
| ThriftTransport | ❌ | ✅ | Basse - sasl/nosasl |

### Configuration Serveur
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| HiveServerType | ❌ | ✅ | Très basse - HiveServer2 uniquement |
| Catalog | ❌ | ✅ | Basse - équivalent à DATABASE |
| DefaultStringColumnLength | ❌ | ✅ | Basse - gestion interne OK |

### Options Avancées
| Paramètre | Argus | CData/Simba | Priorité |
|-----------|-------|-------------|----------|
| UseNativeQuery | ❌ | ✅ | Basse - queries déjà natives |
| AsyncExecPollInterval | ❌ | ✅ | Basse - pas d'async pour l'instant |
| GetTables_SQL | ❌ | ✅ | Basse - SQLTables déjà implémenté |
| DescribeParam | ❌ | ✅ | Basse - params pas supportés |

## 🎯 Recommandations

### Priorité HAUTE - Bloquant en entreprise
1. **Support Kerberos / GSSAPI / SPNEGO** (KrbRealm, KrbHostFQDN, KrbServiceName)
   - **Obligatoire** sur la quasi-totalité des clusters CDP/CDH sécurisés : sans
     Kerberos, le driver ne peut pas se connecter à la majorité des déploiements
     Hadoop d'entreprise. Ce n'est PAS une option « environnements très sécurisés ».
   - Nécessite libkrb5-dev
2. **OAuth2 / OIDC / JWT** (HTTP mode)
   - Standard de fait pour la BI cloud et les passerelles modernes (Knox + JWT).
   - Indispensable pour l'interopérabilité avec les déploiements récents.

Voir `docs/ROADMAP.md` (Phase 1 — Authentification entreprise).

### Priorité MOYENNE
1. **LDAP** sur SASL (complément de PLAIN).
2. **TransportMode HTTP pour Impala** (déjà disponible pour Hive).

### Priorité BASSE - Non nécessaires
- DefaultStringColumnLength: géré automatiquement
- UseNativeQuery: toutes les queries sont natives
- AsyncExecPollInterval: pas d'async pour l'instant
- HiveServerType: HiveServer2 uniquement supporté

## ✨ Avantages d'Argus vs Commercial

Notre driver a des fonctionnalités que CData/Simba n'ont PAS:

1. ✅ **Multi-backend** (Hive + Impala + Trino dans un seul driver)
2. ✅ **Retry automatique** avec délai configurable
3. ✅ **Application Name** pour traçabilité
4. ✅ **SSLVerify** pour contrôle granulaire SSL
5. ✅ **Logging avancé** (7 niveaux vs 2-3 pour commercial)
6. ✅ **Open Source** (vs $500-2000/licence commercial)

## 📊 Conclusion

**Couverture**: large pour les paramètres de connexion de base, SSL, timeouts,
résilience et performance.

**Statut**: utilisable pour les clusters Hive **non kerberisés** (dev/test, NOSASL,
PLAIN/LDAP-over-SSL) et pour Hive derrière Knox en mode HTTP. **PAS encore
production-ready pour la majorité des clusters d'entreprise**, qui imposent
Kerberos.

**Manque bloquant**:
- **Kerberos/GSSAPI** — requis par la plupart des déploiements Hadoop sécurisés.
- **OAuth2/OIDC/JWT** — requis pour la BI cloud et les passerelles modernes.

Argus se distingue sur le retry, le logging et le multi-backend, mais
l'authentification entreprise doit être livrée avant de revendiquer la parité
avec les drivers commerciaux. Voir `docs/ROADMAP.md`.
