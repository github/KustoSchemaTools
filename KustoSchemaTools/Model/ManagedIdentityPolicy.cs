using KustoSchemaTools.Changes;
using Newtonsoft.Json;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Parser;

namespace KustoSchemaTools.Model
{
    public class ManagedIdentityPolicy
    {
        public string ObjectId { get; set; }
        public List<string> AllowedUsages { get; set; } = new List<string>();

        /// <summary>
        /// Creates a single script that sets managed identity policy for all provided identities.
        /// Uses one combined command to avoid duplicate Kind keys in the diff pipeline.
        /// </summary>
        public static DatabaseScriptContainer CreateCombinedScript(string databaseName, List<ManagedIdentityPolicy> policies)
        {
            var policyObjects = policies
                .OrderBy(p => p.ObjectId)
                .Select(p => new { p.ObjectId, AllowedUsages = string.Join(", ", p.AllowedUsages.OrderBy(u => u)) })
                .ToArray();
            var json = JsonConvert.SerializeObject(policyObjects, Serialization.JsonPascalCase);
            return new DatabaseScriptContainer("ManagedIdentityPolicy", 80, $".alter-merge database {databaseName.BracketIfIdentifier()} policy managed_identity ```{json}```");
        }
    }
}
