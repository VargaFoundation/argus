// Connecteur Power BI de navigation pour le driver ODBC Argus.
// Expose Argus.Feed(connectionString) = Odbc.DataSource(...) : c'est l'arbre
// "Navigateur" de Power BI (catalogue > schema > table). HierarchicalNavigation=false
// donne la liste plate des tables (colonnes Name/Item/Schema/Catalog/Data/Kind).
section Argus;

[DataSource.Kind="Argus"]
shared Argus.Feed = (connectionString as text) as table =>
    Odbc.DataSource(connectionString, [HierarchicalNavigation = false]);

Argus = [
    Authentication = [
        Anonymous = [],
        UsernamePassword = []
    ]
];
