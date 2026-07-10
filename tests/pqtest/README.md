# Tests Power BI headless (PQTest)

Valide le driver ODBC Argus **à travers le moteur mashup / Power Query réel de Power BI**,
sans ouvrir Power BI Desktop et sans le clic interactif de saisie des identifiants.

C'est la couche au-dessus de la sonde de folding ODBC (`tests/` côté C) : ici on exerce
le moteur M de Power BI lui-même — credentials, Navigateur, exécution de requêtes.

## Pourquoi

Power BI n'envoie pas seulement vos `SELECT`. Son moteur interroge le driver sur ses
capacités (`SQLGetInfo`), construit le Navigateur (`SQLTables`/`SQLColumns`) et **génère
lui-même du SQL** (query folding). PQTest (`Microsoft.PowerQuery.SdkTools`) exécute des
requêtes M via ce moteur en headless, avec les identifiants fournis en JSON — la façon
Microsoft de tester/certifier une source ODBC en CI.

## Prérequis

- **Windows** (PQTest et le moteur mashup sont Windows-only), .NET, PowerShell.
- Le driver **« Argus ODBC Driver »** installé (installeur signé des releases).
- Un backend joignable. Défaut : **Trino** sur `localhost:8080`, catalogue `tpch`
  (`docker compose -f tests/integration/docker-compose.yml up -d trino`).

## Lancer

```powershell
pwsh -File tests/pqtest/run-pqtest.ps1
# ou contre un autre hôte :
pwsh -File tests/pqtest/run-pqtest.ps1 -TrinoHost 10.0.0.5 -TrinoPort 8080
```

Le script récupère PQTest depuis NuGet, compile les connecteurs (`connectors/*.pq` → `.mez`),
pose un credential **Anonymous** dans un store chiffré (`credential-template` →
`set-credential`), puis `compare` chaque requête `queries/*.query.pq` à son golden `.pqout`.
Code de sortie = nombre d'échecs.

## Contenu

| Fichier | Rôle |
|---|---|
| `connectors/Argus.pq` | Connecteur « requête native » : `Argus.Query(conn, sql)` = `Odbc.Query`. |
| `connectors/ArgusNav.pq` | Connecteur « Navigateur » : `Argus.Feed(conn)` = `Odbc.DataSource`. |
| `connectors/ArgusFold.pq` | Connecteur « Navigateur + folding » : `Odbc.DataSource` avec `SqlCapabilities.LimitClauseKind`. |
| `queries/trino-count.query.pq` | `SELECT count(*)` via requête native → golden `{{25}}`. |
| `queries/trino-filter.query.pq` | `WHERE regionkey=1 ORDER BY name` → 5 lignes. |
| `queries/trino-navigator.query.pq` | Le Navigateur liste bien les tables (golden `80`). |
| `queries/trino-folding.query.pq` | Navigateur → filtre + tri + `FirstN` foldés (via `ArgusFold`). |

## Ce que ces tests établissent (v0.5.9, 2026-07)

- ✅ **Connexion + identifiants** : le moteur mashup passe le gate credentials
  (Anonymous) et se connecte au driver. C'était le seul point réputé non automatisable.
- ✅ **Requête SQL native** (`Odbc.Query`) : renvoie des données correctes et typées.
  C'est le chemin « requête SQL / options avancées » de Power BI.
- ✅ **Navigateur** (`Odbc.DataSource`) : liste les 80 objets de `tpch`
  (catalogue/schéma/table), identifiants correctement quotés en `"..."` (le fix
  `SQL_IDENTIFIER_QUOTE_CHAR` backend-aware de v0.5.9 est visible ici).

## Query folding via le Navigateur — résolu par un connecteur dédié

Avec un connecteur **minimal**, `Odbc.DataSource` nu ne folde jamais `LIMIT`/top-N :
le moteur répond « Nous n'avons pas pu replier l'expression sur la source » et fait le
tri/limit **localement** (le WHERE, lui, foldait déjà). Ce n'est pas un défaut du driver
— c'est une limite d'architecture de Power Query : le pushdown de `LIMIT`/`TOP` exige un
connecteur qui déclare **`SqlCapabilities.LimitClauseKind`**.

`connectors/ArgusFold.pq` le fait (Trino → `LimitClauseKind.AnsiSql2008`, soit
`OFFSET … ROWS FETCH NEXT … ROWS ONLY`), et `queries/trino-folding.query.pq` exerce le
chemin point-and-click complet (Navigateur → filtre → tri → `FirstN`). C'est le même
mécanisme que le **connecteur produit** livrable dans
[`connectors/powerbi/`](../../connectors/powerbi/) — à compiler en `.mez` et installer
dans Power BI Desktop pour le folding complet et **DirectQuery**.

> Le `compare` de PQTest vérifie le **résultat** (identique foldé ou non). Pour prouver
> que le tri/limit est réellement **poussé en SQL**, regarde *Afficher la requête native*
> dans Power Query, ou `system.runtime.queries` côté Trino. La sonde C
> `tests/integration/test_bi_folding.c` vérifie, elle, que le SQL généré (avec le
> quote-char et `OFFSET…FETCH` annoncés) s'exécute bien côté serveur.

## Notes d'implémentation

- Un seul membre `shared [DataSource.Kind=...]` par `.mez` : PQTest ne résout pas la
  source quand plusieurs fonctions data-source cohabitent dans un connecteur compilé à la
  main (sans projet `.mproj`). D'où deux `.pq` séparés.
- MakePQX peut lever une exception de sérialisation en fin d'exécution sur certains
  runtimes .NET ; le `.mez` est écrit **avant**, donc le runner tolère le code de sortie
  et vérifie la présence du `.mez`.
- Les identifiants sont liés au **chemin normalisé** de la source (qui inclut le SQL pour
  `Odbc.Query`) : le runner refait `set-credential` par requête.
