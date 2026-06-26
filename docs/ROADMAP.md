# Argus ODBC — Analyse produit & Roadmap d'évolution

Ce document propose une analyse critique du driver Argus (forces, faiblesses, position
concurrentielle, tendances 2025-2026) et une roadmap d'évolution priorisée. Il couvre
quatre axes — authentification entreprise, performance/Arrow, nouveaux backends,
conformité ODBC — et l'ensemble des moteurs cibles envisagés (Spark, Flink, MySQL-wire,
Druid, Pinot, TDengine, Arrow Flight SQL).

## Résumé exécutif

Argus est un driver ODBC open-source en C11 (~19 500 LOC + ~6 000 LOC de tests)
connectant les outils BI à des moteurs SQL big-data via une architecture à deux couches
(API ODBC `src/odbc/` + backends pluggables `src/backend/`). Cinq backends existent :
Hive, Impala (Thrift CLI), Trino, Phoenix (HTTP/JSON), Kudu (client C++ natif).

L'ingénierie est solide (multi-plateforme, packaging signé, CI, pooling, Unicode complet,
logging structuré) et le **positionnement est unique** : aucun driver ODBC open-source
natif et cross-platform ne couvre aujourd'hui Hive + Impala + Trino, et Trino n'a pas de
driver ODBC officiel.

**Mais le produit se survend.** Le manque d'authentification entreprise (Kerberos,
OAuth2/OIDC) — actuellement classé « priorité moyenne » dans la documentation — est en
réalité le blocage n°1 : il rend Argus inutilisable sur la majorité des clusters Hadoop
sécurisés et sur la BI cloud. C'est la priorité absolue de la roadmap.

---

## 1. Diagnostic critique de l'existant

### Ce qui est solide
- **Architecture vtable propre** (`include/argus/backend.h`) : 14 opérations obligatoires
  + 2 optionnelles. `connect()` reçoit `argus_dbc_t*`, donc un backend peut déjà lire des
  attributs de connexion étendus — point d'entrée naturel pour une authentification riche.
- **Ingénierie produit** au-dessus de la moyenne open-source : packaging natif signé
  (deb/rpm/pkg/NSIS), CI GitHub Actions multi-OS + Codecov, pooling de connexions
  configurable, support Unicode W-suffix complet, logging 7 niveaux.
- **Créneau réel et peu disputé** : le seul concurrent OSS natif (`trinodb/trino-odbc`)
  est Windows-only, read-only et de conformance partielle.

### Faiblesses critiques
- **Authentification = blocage n°1, sous-évalué.** Les fichiers `*_PARAMETERS_COMPARISON.md`
  classent Kerberos en « priorité moyenne / environnements très sécurisés uniquement » et
  concluent « ~95% production-ready, aucun manque HAUTE priorité ». C'est inexact pour le
  marché cible : la quasi-totalité des clusters CDP/CDH Hive/Impala imposent
  Kerberos/GSSAPI ; côté cloud (Trino/Databricks), OAuth2/OIDC + SSO navigateur est le
  standard de fait. Aucun des 5 backends ne fait Kerberos, LDAP, OAuth2 ou JWT
  (`auth_mechanism` se limite à NOSASL/PLAIN ; Trino/Phoenix/Kudu ignorent même le mot
  de passe).
- **Fetch row-oriented.** `fetch_results()` parse ligne par ligne. Les concurrents
  (Databricks Cloud Fetch) font du fetch colonnaire Arrow (~12× le débit). De plus
  l'**array fetch ODBC** (`SQL_ATTR_ROW_ARRAY_SIZE` lu mais non honoré) n'est pas
  implémenté — pénalisant pour les gros extracts BI.
- **Conformité ODBC incomplète** : `SQLSetPos`, `SQLBulkOperations`, bookmarks → HYC00 ;
  async partiel ; `SQLDescribeParam` stub ; curseurs forward-only (+ cache scroll).
  `get_primary_keys`/`get_statistics` absents pour Hive et Impala.
- **HTTP/Knox non exposé.** La couche `src/backend/common/thrift_http.c` existe mais le
  paramètre `TransportMode` (binary/HTTP) n'est pas exposé — or beaucoup de déploiements
  Hive sont derrière Apache Knox en mode HTTP.
- **Backend Kudu discutable.** Kudu se requête normalement via Impala. Le parser SQL
  maison (`kudu_sql_parser.c`, 461 lignes) est une dette de maintenance fragile et
  introduit du C++ dans une base C. À reconsidérer.
- **Pas de suite de benchmarks** ni de tracing distribué (OpenTelemetry).

---

## 2. Concurrence & tendances (2025-2026)

- **Simba → insightsoftware** est le moteur OEM dominant (white-label dans Cloudera,
  ancien Databricks, BigQuery). CData / Progress DataDirect sont les challengers.
  Différenciateurs payants : Kerberos, OAuth2/OIDC/SSO, **fetch Arrow**, pushdown SQL.
- **Databricks** propose désormais un driver JDBC open-source (Apache-2.0, v3.x) avec
  OAuth + Cloud Fetch — signal que l'OSS + Arrow + OAuth devient la norme.
- **Apache Arrow ADBC / Flight SQL** : momentum réel mais positionné comme **supplément,
  pas remplacement** d'ODBC. Signal fort : **Power BI/Fabric bascule vers ADBC par défaut
  et retirera les drivers ODBC embarqués (service fin 2026, Desktop printemps 2027)** —
  recouvre les backends d'Argus. ⇒ garder les internals colonnaires/Arrow-friendly ; un
  chemin Flight SQL/ADBC est un item roadmap crédible à moyen terme.
- **Auth = plancher entreprise** : OAuth2 auth-code + SSO navigateur, client-credentials
  (M2M), device-code (headless), OIDC discovery, JWT/bearer + refresh, key-pair JWT,
  SAML via external browser, **+ Kerberos/GSSAPI/SPNEGO on-prem** (qui ne décline pas, il
  se complète par LDAP/SAML/JWT).

---

## 3. Opportunités de couverture moteurs (effort vs valeur)

| Moteur | Approche | Réutilise | Effort | Valeur |
|---|---|---|---|---|
| **Spark** | endpoint HiveServer2 (Spark Thrift Server, port 10000) | backend **Hive tel quel** | Très faible (valider + doc + test) | Élevée |
| **Flink** | endpoint `hiveserver2` du SQL Gateway (FLIP-223, Hive 2.3) | backend **Hive tel quel** | Très faible (valider + doc) | Moyenne-élevée |
| **StarRocks / Doris / ClickHouse** | nouveau backend **protocole MySQL-wire** | rien (lib `libmariadb`/`mysqlclient`) | Moyen | Très élevée (3 moteurs d'un coup) |
| **Apache Druid** | protocole **Avatica** (comme Phoenix) | code Avatica de **Phoenix** | Faible-moyen | Moyenne |
| **Apache Pinot** | backend HTTP/JSON neuf | infra curl+json-glib | Moyen-élevé | Moyenne (plus gros manque OSS ODBC) |
| **Arrow Flight SQL** | backend gRPC/Arrow → Dremio, InfluxDB 3, Doris, StarRocks | rien (nouvelle dépendance Arrow/Flight) | Élevé | Élevée (modernité, futur ADBC) |
| **TDengine** | propre protocole natif/WebSocket | rien | Élevé | Faible (niche, driver déjà fourni par l'éditeur) |

> **Note** : **Spark Connect (gRPC, Spark 4.0) n'est PAS adressable en ODBC** — c'est une
> API DataFrame, pas un wire protocol SQL. Le seul chemin ODBC vers Spark reste le Thrift
> Server.

---

## 4. Roadmap priorisée

Séquencement par ratio valeur/effort et par dépendances. Chaque phase est livrable
indépendamment.

### Phase 0 — Gains immédiats  ✅ LIVRÉE

> **État** : terminée. Build vert, 18/18 tests unitaires OK. Au passage, deux
> régressions du working tree ont été corrigées : conflits de merge non résolus
> (`handle.h`, `connect.c`) et feature « Thrift-over-HTTP » à moitié câblée
> (`thrift_http.c` absent des sources CMake, curl non détecté pour les backends
> Thrift, helpers `thrift_serialize` `static` mais utilisés en cross-TU).

1. ✅ **Spark + Flink** : documentés (`docs/CONFIGURATION.md`) comme cibles du backend
   Hive, avec tests d'intégration `test_spark_*`/`test_flink_*` (compilés) et services
   `spark-thrift`/`flink-sql-gateway` ajoutés au `docker-compose.yml` (templates à
   valider sous Docker).
2. ✅ **`TransportMode` (binary/HTTP)** : proprement conditionné à libcurl
   (`ARGUS_THRIFT_HTTP`/`ARGUS_HAS_THRIFT_HTTP`), transport opaque + stubs sans curl.
   Hive : HTTP complet. Impala : HTTP **détecté et rejeté proprement** (implémentation
   HTTP restant à faire — voir Phase 0 résiduelle).
3. ✅ **Documentation corrigée** : Kerberos/OAuth reclassés en priorité HAUTE et
   « production-ready » nuancé dans les trois `*_PARAMETERS_COMPARISON.md`.

**Phase 0 résiduelle** (non bloquante) : implémenter le transport HTTP **Impala**
(miroir de Hive sur session/query/fetch/metadata) ; valider les services Docker
Spark/Flink en conditions réelles.

### Phase 1 — Authentification entreprise (déblocage majeur)
*Fichiers : `src/odbc/connect.c` (parsing params), `src/backend/*/*_session.c`, nouveau
`src/backend/common/auth_*.c`.*

> **Contrainte technique majeure (à intégrer dans le séquencement)** : la liaison
> **`thrift_c_glib` ne fournit AUCUN transport SASL/GSSAPI** (contrairement aux
> bindings C++/Java). Le Kerberos sur le chemin **Thrift binaire** Hive/Impala
> implique donc d'écrire une couche de framing SASL/GSS-API maison par-dessus le
> transport thrift_c_glib — effort lourd et délicat. **Recommandation revue** :
> attaquer d'abord l'authentification sur les chemins **HTTP** (où libcurl gère
> nativement SPNEGO/Negotiate, Bearer/JWT et OAuth2), c.-à-d. Trino + Hive/Impala
> HTTP, avant le Kerberos-sur-Thrift-binaire.

1. ✅ **SPNEGO/Negotiate sur HTTP** via libcurl (`CURLAUTH_NEGOTIATE`) — **IMPLÉMENTÉ**
   pour Hive en mode HTTP (`AuthMech=GSSAPI;TransportMode=HTTP`). Utilise le ticket
   du cache Kerberos (`kinit`). Code dans `src/backend/common/thrift_http.c` (avec
   seek callback pour le ré-envoi du corps après le 401 Negotiate). **Compile-vérifié
   contre libcurl réel** ; GSSAPI sur transport binaire est rejeté proprement
   (Hive + Impala). *Validation runtime en attente du bon mot de passe Kerberos
   (cf. cluster clemlab : Hive HTTP+SPNEGO+SSL confirmé, endpoint renvoie bien
   `401 Negotiate`).*
2. **OAuth2/OIDC + JWT sur HTTP** (Trino d'abord, puis Hive/Impala en mode HTTP) :
   bearer (✅), client-credentials M2M (✅), **refresh automatique du token sur 401
   (✅)** ; restent auth-code + loopback PKCE (SSO navigateur), device-code
   (headless), OIDC discovery. Mutualisé dans `common/`.
   *Le plus demandé pour la BI cloud ; même approche libcurl que SPNEGO.*
3. **Kerberos/GSSAPI sur Thrift binaire** (Hive/Impala) : nécessite une couche SASL
   maison (voir contrainte ci-dessus). Paramètres `KrbRealm`, `KrbHostFQDN`,
   `KrbServiceName`. Optionnel à la compilation (libkrb5/libsasl2).
4. **LDAP / PLAIN sur SASL** complet (Hive/Impala).

### Phase 2 — Performance & Arrow-readiness
1. **Array fetch ODBC** : honorer `SQL_ATTR_ROW_ARRAY_SIZE` (fetch multi-lignes en un
   appel) dans `src/odbc/fetch.c`.
2. **Fetch colonnaire de bout en bout** : généraliser un format colonnaire interne (le
   parsing Hive est déjà colonnaire) pour éviter les conversions row-wise inutiles.
3. **Cloud Fetch / résultats segmentés** : exploiter le spooling Trino v2 (déjà présent,
   `src/backend/trino/trino_spooling.c`) et préparer le pattern Databricks.
4. **Suite de benchmarks** (nouvelle, sous `tests/bench/`).

### Phase 3 — Nouveaux backends à fort levier
1. **Backend MySQL-wire** (`src/backend/mysql/`) → StarRocks + Doris + ClickHouse.
   Dépendance `libmariadb`, auto-détectée. ✅ **Implémenté** (`BACKEND=mysql`,
   `mywire_*` : connect/execute/fetch/metadata + catalogue via `information_schema`).
2. **Backend Druid** (`src/backend/druid/`) en réutilisant le code Avatica de Phoenix
   (factoriser l'Avatica commun dans `src/backend/common/avatica_*.c`).
3. **Backend Pinot** (HTTP/JSON) si la demande se confirme.

### Phase 4 — Conformité ODBC & modernité
1. Compléter `SQLSetPos` (✅ `SQL_POSITION` + `SQL_REFRESH` sur curseur statique),
   `SQLBulkOperations`, `SQLDescribeParam`, async complet. Restent les opérations
   `SQL_UPDATE`/`SQL_DELETE`/`SQL_ADD` (génération de DML, peu pertinent pour les
   moteurs append-mostly).
2. Ajouter `get_primary_keys`/`get_statistics` pour Hive et Impala.
3. **Backend/chemin Arrow Flight SQL** (`src/backend/flightsql/`) → Dremio, InfluxDB 3,
   *(implémenté derrière `ARGUS_BUILD_FLIGHTSQL` ; compile contre Arrow C++ 24
   (GCC 14 + C++20) ; **validé end-to-end contre InfluxDB 3 Core** — SELECT (avec
   timestamps), SQLColumns, SQLTables, SQLPrimaryKeys, SQLGetTypeInfo. Reste :
   Dremio/Doris + auth runtime. Voir `docs/FLIGHTSQL_DESIGN.md`)*
   Doris, StarRocks ; fondations d'une future surface **ADBC** (anticipe la bascule
   Power BI 2026-2027).
4. Décision sur **Kudu** : déprécier le parser SQL maison ou l'assumer comme niche.

---

## 5. Fichiers clés concernés

- Interface backend : `include/argus/backend.h` (signature `connect` déjà compatible auth
  riche via `dbc`).
- Auth : `src/odbc/connect.c`, `src/backend/*/*_session.c`, nouveau
  `src/backend/common/auth_*.c`.
- Performance : `src/odbc/fetch.c`, `src/backend/*/*_fetch.c`,
  `src/backend/trino/trino_spooling.c`.
- Transport HTTP : `src/backend/common/thrift_http.c` (présent, à exposer).
- Nouveaux backends : `src/backend/{mysql,druid,pinot,flightsql}/` +
  `src/backend/backend.c` (registre) + `CMakeLists.txt` (auto-détection des dépendances).
- Documentation/tests : `docs/ADDING_BACKENDS.md`,
  `tests/integration/docker-compose.yml`, `*_PARAMETERS_COMPARISON.md`.
