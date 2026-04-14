using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoDatabasePrincipalLoader : IKustoBulkEntitiesLoader
    {
        const string script = ".show database principals | project Role, PrincipalDisplayName, PrincipalFQN";

        public async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            var response = await kusto.Client.ExecuteQueryAsync(databaseName, script, new ClientRequestProperties());
            var rows = response.As<PrincipalRawRow>();
            var principals = PrincipalParser.ParsePrincipals(rows);

            database.Admins = principals.GetValueOrDefault("Admin", new List<AADObject>());
            database.Users = principals.GetValueOrDefault("User", new List<AADObject>());
            database.Ingestors = principals.GetValueOrDefault("Ingestor", new List<AADObject>());
            database.Viewers = principals.GetValueOrDefault("Viewer", new List<AADObject>());
            database.UnrestrictedViewers = principals.GetValueOrDefault("UnrestrictedViewer", new List<AADObject>());
            database.Monitors = principals.GetValueOrDefault("Monitor", new List<AADObject>());
        }
    }
}
