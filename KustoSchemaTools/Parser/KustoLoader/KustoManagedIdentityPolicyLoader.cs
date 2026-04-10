using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Newtonsoft.Json;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoManagedIdentityPolicyLoader : IKustoBulkEntitiesLoader
    {
        const string script = @"
.show database policy managed_identity
| project Policies = parse_json(Policy)
| mv-expand Policy = Policies
| project ObjectId = tostring(Policy.ObjectId), AllowedUsages = tostring(Policy.AllowedUsages)";

        public async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            var response = await kusto.Client.ExecuteQueryAsync(databaseName, script, new ClientRequestProperties());
            var rows = response.As<ManagedIdentityRow>();
            if (database.Policies == null)
                database.Policies = new DatabasePolicies();
            database.Policies.ManagedIdentity = rows
                .Select(r => new ManagedIdentityPolicy
                {
                    ObjectId = r.ObjectId,
                    AllowedUsages = r.AllowedUsages
                        .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                        .OrderBy(u => u)
                        .ToList()
                })
                .OrderBy(p => p.ObjectId)
                .ToList();
        }

        private class ManagedIdentityRow
        {
            public string ObjectId { get; set; }
            public string AllowedUsages { get; set; }
        }
    }
}
