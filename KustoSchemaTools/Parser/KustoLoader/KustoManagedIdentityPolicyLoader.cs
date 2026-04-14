using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoManagedIdentityPolicyLoader : IKustoBulkEntitiesLoader
    {
        const string script = ".show database policy managed_identity | project Policy";

        public async Task Load(Database database, string databaseName, KustoClient kusto)
        {
            var response = await kusto.Client.ExecuteQueryAsync(databaseName, script, new ClientRequestProperties());
            var rows = response.As<ManagedIdentityRawRow>();
            var policyJson = rows.FirstOrDefault()?.Policy ?? "[]";

            if (database.Policies == null)
                database.Policies = new DatabasePolicies();
            database.Policies.ManagedIdentity = ManagedIdentityPolicy.ParseFromPolicyJson(policyJson);
        }

        private class ManagedIdentityRawRow
        {
            public string Policy { get; set; }
        }
    }
}
