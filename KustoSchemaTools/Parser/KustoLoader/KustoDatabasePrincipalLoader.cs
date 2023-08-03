using Kusto.Data.Common;
using KustoSchemaTools.KustoTypes.DB;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoDatabasePrincipalLoader : IKustoBulkEntitiesLoader
    {
        const string script = @"
.show database principals
| project id=PrincipalFQN, name=trim(' ',tostring(split(PrincipalDisplayName,'(')[0])), Role=tostring(split(Role,' ')[-1])
| where Role !startswith 'All'
| project AAObject=bag_pack('name',name,'id',id), Role
| summarize Users = make_list(AAObject) by Role
";

        public async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            var response = await kusto.Client.ExecuteQueryAsync(databaseName, script, new ClientRequestProperties());
            var principals = response.As<PrincipalRow>().ToDictionary(itm => itm.Role, itm => itm.Users);
            database.Admins = principals.ContainsKey("Admin") ? principals["Admin"] : new List<AADObject>();
            database.Users = principals.ContainsKey("User") ? principals["User"] : new List<AADObject>();
            database.Ingestors = principals.ContainsKey("Ingestor") ? principals["Ingestor"] : new List<AADObject>();
            database.Viewers = principals.ContainsKey("Viewer") ? principals["Viewer"] : new List<AADObject>();
            database.UnrestrictedViewers = principals.ContainsKey("UnrestrictedViewer") ? principals["UnrestrictedViewer"] : new List<AADObject>();
        }
    }
}
