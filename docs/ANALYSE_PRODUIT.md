# Analyse critique du produit — Argus (édition open-source)

*Audit produit et technique, août 2026. Périmètre : ce dépôt (`VargaFoundation/argus`). L'analyse de l'édition entreprise et de la stratégie open-core se trouve dans `docs/ANALYSE_PRODUIT.md` du dépôt `argus-entreprise`.*

---

## 1. Synthèse

Argus est un driver ODBC 3.8 multi-backend (~30 000 LOC C11, 10 moteurs annoncés) qui vise la parité avec les drivers commerciaux Simba/Starburst. Le socle est réel et souvent bien fait : vtable backend propre, diagnostics ODBC cohérents, async statement-level, descripteurs, dialecte/escapes `{fn}` méticuleux, décodage Trino sans DOM avec kill-switch, ASan/UBSan et CodeQL en CI, harness TDVT.

Mais le produit **se survend** (le propre `docs/ROADMAP.md` le dit) et souffre de trois familles de problèmes :

1. **Des bugs de correction silencieux** — corruption mémoire connue (`SQL_ATTR_ROW_BIND_TYPE`), échappement de paramètres non-dialecté (corruption de données sur Trino/Phoenix/Druid/Pinot), transactions simulées, `SQLCancel`/`is_alive` factices sur plusieurs backends.
2. **Un écart claims/réalité** — « 107 entry points » (réellement 104), « Parity » sur la GUI DSN qui n'existe pas, TDVT 91,4 % non reproductible, « 10 backends » dont 4 sont de seconde classe, docs contradictoires entre elles.
3. **Une industrialisation incomplète** — 21/33 fichiers de tests d'intégration jamais exécutés en CI, zéro fuzzing sur 4 parseurs maison, aucune release taguée, aucun canal de distribution (brew/apt/winget), gouvernance opaque (1 auteur, historique tronqué).

Rien d'irrécupérable : la plupart des correctifs de crédibilité tiennent en semaines, pas en trimestres. Le détail et la priorisation suivent.

---

## 2. Forces (à préserver)

- **Abstraction backend par vtable** (`include/argus/backend.h`) : 20 membres, registre unique, enregistrement compile-time. Le bon design pour un driver multi-moteur.
- **Dialecte SQL et traduction d'escapes ODBC** (`src/odbc/dialect.c`, `escape.c`) : la vraie différenciation face au « Other Databases (ODBC) » générique — soignée, testée, honnête sur ce qui est vérifié moteur par moteur (`docs/BI_TOOLS.md`).
- **Perf Trino** : décodage DOM-free (~65 % plus rapide) avec kill-switch `ARGUS_TRINO_NOFASTJSON` et harness de vérification byte-identique — la bonne façon d'optimiser.
- **Async réel** (`SQL_AM_STATEMENT`, `SQLCompleteAsync`, `SQLCancelHandle`), descripteurs ODBC réels, pool de connexions.
- **Hygiène de base** : 0 `strcpy`/`sprintf`/`strcat` dans `src/`, diagnostics normalisés `[Argus]`, ASan+UBSan en CI (tests unitaires), CodeQL.
- **Docs fortes là où elles existent** : `BI_TOOLS.md` et le README TDVT sont spécifiques, reproductibles et honnêtes sur les limites.
- **Télémétrie à whitelist stricte** (`docs/TELEMETRY.md`) : opt-in, SQLSTATE seulement, jamais d'identifiants — le design est bon (deux réserves en §4.6).
- **Atout unique sous-exploité** : endpoints Google entièrement configurables pour BigQuery, y compris clouds souverains S3NS — capacité qu'aucun driver fermé n'expose.

---

## 3. Défauts de correction (bugs, par gravité)

### 3.1 `SQL_ATTR_ROW_BIND_TYPE` accepté puis ignoré — corruption mémoire
`src/odbc/attr.c` accepte le row-wise binding puis écrit column-wise. Une application qui demande du row-wise reçoit `SQL_SUCCESS` puis des écritures hors de ses structures. Le `docs/ROADMAP.md` du projet le qualifie lui-même de « défaut le plus dangereux du driver », mitigé seulement par « les outils BI bindent par colonne ». **Correctif : implémenter le row-wise ou refuser l'attribut (`HYC00`/`HY092`) — jamais accepter-puis-ignorer.**

### 3.2 Échappement de paramètres non-dialecté — corruption de données
`sql_escape_string` (`src/odbc/execute.c:36`) double systématiquement le backslash (`\` → `\\`), sémantique MySQL/Hive/BigQuery. Trino, Phoenix, Druid et Pinot utilisent les littéraux ANSI où `\` n'est **pas** un caractère d'échappement : un paramètre lié contenant `C:\path` devient la chaîne littérale `C:\\path` côté serveur. Corruption silencieuse sur toute requête paramétrée. Le contraste est frappant : `escape.c`/`dialect.c` sont méticuleusement par-dialecte, le chemin des paramètres ne l'est pas. En prime, `count_param_markers` (`execute.c:19`) compte les `?` dans les commentaires SQL (`--`, `/* */`). **Correctif : router l'échappement par le dialecte du backend et tokeniser les commentaires.**

### 3.3 Transactions simulées
`SQLEndTran` (`src/odbc/attr.c:570`) retourne `SQL_SUCCESS` inconditionnellement (« Hive doesn't support transactions - just return success »). Le driver embarque pourtant des backends MySQL/MariaDB et Trino où un rollback demandé et silencieusement « réussi » signifie un commit implicite. `SQL_TXN_CAPABLE=SQL_TC_NONE` atténue mais ne supprime pas le piège. **Correctif : `SQL_ERROR`/`HYC00` quand le backend ne supporte pas, vrai commit/rollback sur mysql-wire.**

### 3.4 `SQLCancel` et `is_alive` factices
- `druid_cancel`, `pinot_cancel`, `mywire_cancel` retournent 0 sans rien faire — le README annonce pourtant « Cancel running queries across all backends ».
- `druid_is_alive`, `pinot_is_alive`, `bq_is_alive` = `return raw != NULL;`. Or `src/odbc/pool.c:127` s'en sert comme gate de vitalité : le pool redistribue des connexions mortes sur ces trois backends.

**Correctif : un no-op doit retourner « non supporté », pas « succès » ; implémenter un ping léger (HEAD/health endpoint) là où c'est possible.**

### 3.5 Thread-safety partielle
Les macros de lock (`include/argus/handle.h:287`) ne sont utilisées que dans 2 des 21 fichiers de `src/odbc/` (`execute.c`, `fetch.c`). `SQLGetDiagRec`, `SQLSetStmtAttr`, les fonctions catalogue, `desc.c`, `handle.c` sont non protégés face à un fetch concurrent — ODBC exige la thread-safety par handle. `argus_env_t` n'a aucun mutex. **Correctif : audit systématique de la surface ODBC, lock par handle partout où l'état partagé est touché.**

### 3.6 Empreinte mémoire des handles
`argus_diag_t` = 64 enregistrements × ~1 Ko ≈ 66 Ko **embarqués par valeur** dans chaque handle ; un statement embarque 1 diag + 4 descripteurs avec chacun leur diag + `param_bindings[256]` ≈ **345 Ko par `SQLAllocHandle(SQL_HANDLE_STMT)`**. Un outil BI avec 200 statements consomme ~69 Mo de diagnostics vides. **Correctif : allocation paresseuse des diags et des tableaux de bindings.**

### 3.7 Allocateurs mélangés
207 `malloc`/`calloc` + 212 `strdup` + 491 `free` côtoient 155 `g_malloc`/`g_free`. Mélanger `free()` libc et mémoire GLib est un UB documenté, et une vraie classe de crash sur Windows (heaps CRT distincts). **Correctif : choisir une convention (GLib partout, ou libc partout) et l'imposer par revue/lint.**

---

## 4. Écart entre les claims et la réalité

### 4.1 « 107 entry points » — c'est 104
`grep` sur `src/**/*.c` et `include/argus/odbc_api.h` donne exactement **104** points d'entrée `SQL*`. `docs/SIMBA_PARITY.md` montre l'erreur d'arithmétique : « 107 (was 104; +3 W-descriptor) » — or `SQLGetDescFieldW`/`SetDescFieldW`/`GetDescRecW` étaient déjà comptés dans les 104. Le différenciateur-titre « 107 vs 89 » est en réalité « 104 vs 89 » — toujours favorable, mais faux tel quel, et le nombre brut d'exports est de toute façon une métrique faible.

### 4.2 « Parity » sur la configuration DSN — indéfendable
`src/odbc/setup.c` est volontairement **sans interface** : un analyste Windows qui ouvre l'administrateur ODBC, clique « Ajouter » et choisit Argus obtient une erreur. Simba/CData/Starburst livrent tous un dialogue à onglets (auth, SSL, avancé). `docs/SIMBA_PARITY.md` note pourtant cette ligne « Parity ». C'est le claim le moins défendable du document, et la friction d'adoption n° 1 sur le poste analyste.

### 4.3 TDVT 91,4 % — plausible mais invérifiable
Bien documenté (méthode, overrides `dialect.tdd`, pièges de licence) mais : un seul connecteur (Trino), suite expressions seulement, **aucun artefact de résultat commité** — le chiffre n'est pas reproductible depuis le dépôt, et il est comparé à une **certification** vendeur.

### 4.4 « 10 backends » — 6 réels, 4 de seconde classe
- Fill-rate vtable : `get_statistics` 1/10 (trino), `get_server_version` 2/10 → `SQL_DBMS_VER` « unknown » pour 8 moteurs ; `get_type_info` NULL pour bigquery/druid ; `get_catalogs` NULL pour druid/pinot.
- 4 backends (phoenix, druid, flightsql, kudu) ont un dialecte volontairement minimal (3 fonctions).
- **Druid** : zéro test d'intégration, jamais validé runtime, cancel no-op, is_alive factice.
- **Kudu** : déprécié par le README, 2 161 LOC dont un parseur SQL maison de 462 lignes, non buildable sur un OS courant (libkudu_client non packagé après Ubuntu 16.04).
- Spark/Flink sont « supportés » via le backend hive mais n'ont ni entrée de dialecte, ni test exécuté.
- 6 fonctions catalogue (`SQLSpecialColumns`, `SQLForeignKeys`, `SQLProcedures`, …) retournent des résultats vides **codés en dur sans hook vtable** — défendable pour Trino/Hive, faux pour MySQL-wire (StarRocks/Doris/MySQL ont des FK et des procédures).
- Pas de prepared statements : `SQLPrepare` stocke une chaîne, les paramètres sont rendus en littéraux texte, y compris sur les protocoles qui savent préparer (mysql-wire, Avatica, Flight SQL).

### 4.5 Docs contradictoires ou périmées
- `SIMBA_PARITY.md` se contredit : la table dit « Parity » sur l'async et le catalogue, la conclusion dit que Simba garde l'avantage sur les deux.
- Les trois `*_PARAMETERS_COMPARISON.md` (en français, le reste en anglais) concluent « PAS production-ready » (Hive/Impala kerberisés) et « PAS au niveau des drivers commerciaux pour la BI cloud » (Trino) — périmés sur Kerberos/OAuth2/async, mais ce sont les documents qu'un évaluateur trouvera.
- Trois versions circulent : `CMakeLists.txt` 0.6.0, dernier bump commité 0.5.9, `CHANGELOG.md` s'arrête à 0.2.0 (et saute 0.3–0.5).
- `ARCHITECTURE.md` décrit 3 backends sur 10 et ignore ADBC, pool, async, descripteurs, dialecte, télémétrie, obs_hooks.

### 4.6 Télémétrie : deux écarts au discours
Le design est privacy-first, mais : (1) `telemetry.c:335` crée un `install_id` machine persistant **même quand la télémétrie est off** (seul `ARGUS_TELEMETRY=0` le supprime) ; (2) l'endpoint `https://telemetry.vargafoundation.org/v1/events` est compilé par défaut (`ARGUS_ENABLE_TELEMETRY=ON`), et la notice de premier lancement ne va que dans le log. À corriger pour que la réalité soit aussi propre que `PRIVACY.md`.

### 4.7 Gouvernance et transparence
- 50 commits, un seul auteur, historique démarrant à « Bump version to 0.5.4 » : ~85 % de la vie du projet est inauditables.
- Pas de `CONTRIBUTING.md`, `SECURITY.md`, `NOTICE`, pas de processus CVE.
- **Aucun tag, aucune release publiée** — alors que README et docs pointent vers la page Releases.
- Le seam `obs_hooks` (`include/argus/obs_hooks.h`) est un point de veto de connexion pré-câblé pour un addon propriétaire (l'historique git est explicite : gate de licence renommé puis « neutralisé »). Légitime en open-core, mais **non documenté** dans le README d'un projet Apache-2.0 — c'est un fait de gouvernance qu'un adoptant doit connaître. Note technique : `__attribute__((weak))` ne s'écrase pas de façon fiable à travers les frontières de bibliothèques partagées et n'existe pas sous MSVC.

---

## 5. Dette technique et industrialisation

### 5.1 Tests : la CI ne teste pas ce qui est risqué
- 213 tests unitaires + 96 d'intégration existent, mais la CI (`.github/workflows/ci.yml:433`) ne lance qu'un sous-ensemble : **21 des 33 fichiers d'intégration ne s'exécutent jamais** — dont TOUT Impala, Phoenix, Kudu, Flight SQL, Spark, Flink, **Kerberos**, ADBC, async, failover.
- 670 LOC de handshake SASL/GSSAPI/SSPI (`thrift_sasl.c`) — code de sécurité — sans aucun test exécuté.
- Druid : zéro test d'intégration. MySQL-wire : zéro test unitaire.
- Les sanitizers ne tournent que sur les tests unitaires, jamais sur le code protocole.
- **Zéro fuzzing** alors que le driver parse à la main : JSON (scanner `sj_*`), wire MySQL, trames SASL, SQL (Kudu). Candidat OSS-Fuzz idéal (gratuit, et un badge de crédibilité).
- Couverture mesurée (lcov/Codecov) mais aucun seuil imposé.

### 5.2 Duplication et frontières de couches
- hive/impala quasi-copies : `hive_types.c` vs `impala_types.c` = 8 lignes de différence ; fetch/metadata ~60 % partagés ; `json_value_to_str` dupliqué druid/pinot.
- curl configuré à la main en 6 endroits, la vérification TLS décidée en **8 sites indépendants** ; `http_client.c` (« client HTTPS partagé ») n'a qu'un consommateur : la télémétrie. **Un choke point TLS unique est aussi un enjeu de sécurité.**
- `argus_dbc` est un god-struct avec des champs `bq_*`, `oauth_*`, `krb_*`, `trino_protocol_version` (`include/argus/handle.h:35-133`) : ajouter un backend = éditer le handle core, ce qui ruine l'intérêt de la vtable. `info.c`/`connect.c` hardcodent des noms de backends. **Correctif : un `void *backend_opts` par backend + table d'options déclarative.**
- `flightsql_backend.cpp:449` initialise la vtable positionnellement (les 9 autres en désigné) : insérer un membre au milieu décale silencieusement tous les pointeurs suivants.

### 5.3 Distribution : zéro canal
Aucune formule Homebrew, aucun dépôt apt/PPA, yum, ni Chocolatey/winget/vcpkg/conda. La distribution = artefacts GitHub Releases… qui n'existent pas (pas de tags). Pas d'ARM64 (ni macOS visé explicitement, ni Linux), pas de manylinux. Face à des Simba pré-embarqués chez les vendeurs, c'est le plafond d'adoption principal — et le moins cher à lever.

### 5.4 Modèle de fetch : le plafond architectural
`argus_cell_t` = une chaîne allouée par cellule (`docs/ADDING_BACKENDS.md:152`) : un résultat 300k × 9 = 2,7 M de mallocs (~350 ns/cellule mesurés par le ROADMAP). Le fast-path I64/F64 et le scanner sans DOM grattent des constantes, mais la réponse structurelle à Simba Arrow/Databricks Cloud Fetch (~12× débit annoncé) est un chemin colonne de bout en bout — voir améliorations.

---

## 6. Améliorations proposées

### Axe 1 — Crédibilité (quick wins, ≤ 1 mois)
| # | Action | Pourquoi |
|---|---|---|
| 1 | Corriger §3.1 (ROW_BIND_TYPE), §3.2 (escaping dialecté + commentaires), §3.3 (SQLEndTran honnête) | Bugs de corruption : disqualifiants en audit client |
| 2 | Rendre cancel/is_alive honnêtes (§3.4) | Fiabilité du pool + véracité du README |
| 3 | Corriger « 107 » → 104, la ligne « Parity » GUI, la conclusion de SIMBA_PARITY ; mettre à jour ou retirer les 3 comparatifs français ; unifier les versions ; réécrire CHANGELOG en notes utilisateur | Un évaluateur qui trouve une erreur factuelle doute de tout le reste |
| 4 | **Publier une première release taguée et signée** (deb/rpm/pkg/exe) | Tout le reste en dépend ; docs et installeurs pointent dans le vide |
| 5 | Ajouter `SECURITY.md`, `CONTRIBUTING.md`, documenter le seam obs_hooks dans le README | Gouvernance minimale d'un projet Apache-2.0 |
| 6 | Télémétrie : ne créer `install_id` qu'au premier opt-in ; notice sur stderr | Aligner la réalité sur PRIVACY.md |

### Axe 2 — Robustesse & industrialisation (1–3 mois)
- **CI** : exécuter les 21 tests d'intégration orphelins (au minimum Impala, Phoenix, Kerberos-KDC, async, failover) ; sanitizers sur l'intégration ; seuil de couverture ; clang-tidy ; **OSS-Fuzz** sur les 4 parseurs.
- **Thread-safety** : campagne de locks sur toute la surface ODBC + mutex env (§3.5) ; test de stress concurrent par handle.
- **Refactoring** : fusionner hive/impala sur un socle commun ; client HTTP/TLS unique (choke point de vérification) ; sortir les champs backend du `argus_dbc` ; allocation paresseuse des diags (§3.6) ; convention d'allocateur unique (§3.7).
- **Distribution** : tap Homebrew, dépôt apt/yum, winget + Chocolatey, ARM64 (macOS Apple Silicon d'abord — c'est le poste analyste moderne). Meilleur ratio impact/effort de tout le backlog.
- **UX Windows** : un vrai dialogue `ConfigDSN` (même minimal : backend, host, port, auth, SSL, bouton Test). Tant qu'il n'existe pas, retirer le « Parity ».

### Axe 3 — Différenciation (3–9 mois)
- **Chemin colonne de bout en bout** : cache de lignes en colonnes (Arrow en interne), conversion à la demande vers les buffers ODBC, et exposition directe via ADBC. C'est la seule réponse structurelle à Cloud Fetch, et l'investissement ADBC existant la rentabilise.
- **Prepared statements réels** là où le protocole les offre (mysql-wire, Avatica, Flight SQL) — supprime la classe d'injections/corruptions du rendu textuel.
- **Finir l'auth entreprise** : valider en runtime SPNEGO/Kerberos (le KDC de test existe), Kerberos sur Thrift binaire (la couche SASL maison est amorcée), OAuth2 browser-SSO démontré contre un IdP réel. C'est LE bloqueur « production-ready » que les propres docs du projet admettent.
- **Resserrer le catalogue** : annoncer 6 backends de classe A (trino, hive, impala, mysql-wire, bigquery, flightsql) + marquer phoenix/pinot/druid « expérimentaux » + **retirer kudu** (déprécié et non buildable). « 6 solides » vend mieux que « 10 inégaux » — et libère de la maintenance.
- **Chaîne BI complète** : certificat CA public + TDVT par connecteur → soumission Tableau Exchange ; certification du connecteur Power BI. La certification est un actif de confiance qu'aucun concurrent open-source n'a.
- **Compléter la vtable** : `get_server_version` partout (SQL_DBMS_VER), hooks catalogue pour FK/procédures sur mysql-wire.

### Axe 4 — Portes ouvertes (paris à évaluer)
- **Souveraineté EU/FR** : BigQuery S3NS + Apache-2.0 + self-hosted = un positionnement que ni Simba ni CData ne peuvent copier. Aujourd'hui : une ligne de README. En faire un axe produit (documentation dédiée, cibles secteur public/banques, SecNumCloud).
- **Dialecte SQLAlchemy** au-dessus du driver (via pyodbc/ADBC) : ouvre Superset, et une partie de l'écosystème Python, à coût faible — les docs actuelles ferment cette porte un peu vite.
- **ADBC comme pari principal** : si le monde BI bascule Arrow-native, `libargus_adbc` est une option d'avance ; le chemin colonne (ci-dessus) est le prérequis.
- **JDBC** : assumé hors scope — défendable (les moteurs ont des JDBC first-party gratuits). À réévaluer seulement si la demande DBeaver/Metabase devient un signal commercial.
- Fenêtre opportuniste : le retrait des connecteurs Hive embarqués de Power BI (documenté dans `docs/ADBC.md`) est une porte d'entrée marketing concrète.

---

## 7. Priorisation recommandée

1. **Semaine 1–4** : Axe 1 en entier (bugs de corruption + véracité des docs + première release). Sans cela, toute évaluation sérieuse échoue.
2. **Mois 2–3** : distribution (brew/apt/winget), CI complète + fuzzing, dialogue DSN Windows.
3. **Mois 3–9** : auth entreprise validée runtime, chemin colonne, resserrage du catalogue, certifications BI.
4. **En continu** : gouvernance (releases régulières, CHANGELOG utilisateur, SECURITY.md vivant).

Le fil conducteur : **la crédibilité d'abord**. Le projet a choisi un positionnement « parité mesurée, honnête » — c'est le bon — mais il faut que chaque claim survive à la vérification, car c'est précisément le terrain sur lequel il attaque Simba.
