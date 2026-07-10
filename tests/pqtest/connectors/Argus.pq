// Connecteur Power BI minimal pour le driver ODBC Argus — chemin "requete native".
// Utilise par les tests PQTest headless (moteur mashup reel de Power BI / Power Query).
// Expose une seule fonction data-source : Argus.Query(connectionString, sql).
//
// NB : un seul membre `shared [DataSource.Kind=...]` par .mez — PQTest ne resout pas
// la source quand plusieurs fonctions data-source cohabitent dans un connecteur compile
// a la main (sans projet .mproj). Le connecteur de navigation est dans ArgusNav.pq.
section Argus;

[DataSource.Kind="Argus"]
shared Argus.Query = (connectionString as text, sql as text) as table =>
    Odbc.Query(connectionString, sql);

Argus = [
    Authentication = [
        Anonymous = [],
        UsernamePassword = []
    ]
];
