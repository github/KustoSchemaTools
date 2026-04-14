using KustoSchemaTools.Changes;
using Newtonsoft.Json;
using KustoSchemaTools.Helpers;
using KustoSchemaTools.Parser;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class ManagedIdentityPolicy
    {
        public string ObjectId { get; set; }
        public List<string> AllowedUsages { get; set; } = new List<string>();

        [YamlIgnore]
        public string ClientId { get; set; }

        /// <summary>
        /// Parses the raw JSON Policy column from .show database policy managed_identity
        /// into a list of ManagedIdentityPolicy objects.
        /// </summary>
        public static List<ManagedIdentityPolicy> ParseFromPolicyJson(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
                return new List<ManagedIdentityPolicy>();

            var rawPolicies = JsonConvert.DeserializeObject<List<RawManagedIdentityPolicy>>(json);
            if (rawPolicies == null)
                return new List<ManagedIdentityPolicy>();

            return rawPolicies
                .Select(p => new ManagedIdentityPolicy
                {
                    ObjectId = p.ObjectId,
                    ClientId = p.ClientId,
                    AllowedUsages = (p.AllowedUsages ?? "")
                        .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                        .OrderBy(u => u)
                        .ToList()
                })
                .OrderBy(p => p.ObjectId)
                .ToList();
        }

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

        private class RawManagedIdentityPolicy
        {
            public string ObjectId { get; set; }
            public string ClientId { get; set; }
            public string AllowedUsages { get; set; }
        }
    }
}
