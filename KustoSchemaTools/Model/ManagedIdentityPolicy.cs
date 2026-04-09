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

        public DatabaseScriptContainer CreateScript(string databaseName)
        {
            var policyObjects = new[] { new { ObjectId = ObjectId, AllowedUsages = string.Join(", ", AllowedUsages) } };
            var json = JsonConvert.SerializeObject(policyObjects, Serialization.JsonPascalCase);
            return new DatabaseScriptContainer("ManagedIdentityPolicy", 80, $".alter-merge database {databaseName.BracketIfIdentifier()} policy managed_identity ```{json}```");
        }
    }
}
