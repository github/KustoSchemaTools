using KustoSchemaTools.Changes;
using KustoSchemaTools.Parser;
using System.Diagnostics.CodeAnalysis;

namespace KustoSchemaTools.Model
{
    public class Database
    {
        public string Name { get; set; }
        public string Team { get; set; } = "";
        public RetentionAndCachePolicy DefaultRetentionAndCache { get; set; } = new RetentionAndCachePolicy();

        public List<AADObject> Monitors { get; set; } = new List<AADObject>();

        public List<AADObject> Viewers { get; set; } = new List<AADObject>();
        public List<AADObject> UnrestrictedViewers { get; set; } = new List<AADObject>();
        public List<AADObject> Users { get; set; } = new List<AADObject>();
        public List<AADObject> Ingestors { get; set; } = new List<AADObject>();
        public List<AADObject> Admins { get; set; } = new List<AADObject>();

        public Dictionary<string, Table> Tables { get; set; } = new Dictionary<string, Table>();

        public Dictionary<string, MaterializedView> MaterializedViews { get; set; } = new Dictionary<string, MaterializedView>();

        public Dictionary<string, Function> Functions { get; set; } = new Dictionary<string, Function>();
        public Dictionary<string, ContinuousExport> ContinuousExports { get; set; } = new Dictionary<string, ContinuousExport>();

        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();

        public Dictionary<string, List<Entity>> EntityGroups { get; set; } = new Dictionary<string, List<Entity>>();

        public Dictionary<string, ExternalTable> ExternalTables { get;  set; } = new Dictionary<string, ExternalTable>();

        public List<Metadata> Metadata { get; set; } = new List<Metadata> { };

        public Deletions Deletions { get; set; } = new Deletions();

        public Dictionary<string, FollowerDatabase> Followers { get; set; } = new Dictionary<string, FollowerDatabase>();

        public DatabasePolicies Policies { get; set; } = new DatabasePolicies();

        public string EscapedName => Name.BracketIfIdentifier();

        /// <summary>
        /// Replaces managed identity ClientId with ObjectId in all principal FQNs.
        /// This prevents phantom diffs when .show database principals returns
        /// ClientId but the YAML stores ObjectId.
        /// </summary>
        public void NormalizePrincipalIds()
        {
            var clientToObject = BuildClientToObjectIdMap();
            if (clientToObject == null || !clientToObject.Any())
                return;

            NormalizePrincipalList(Admins, clientToObject);
            NormalizePrincipalList(Users, clientToObject);
            NormalizePrincipalList(Viewers, clientToObject);
            NormalizePrincipalList(UnrestrictedViewers, clientToObject);
            NormalizePrincipalList(Ingestors, clientToObject);
            NormalizePrincipalList(Monitors, clientToObject);
        }

        internal Dictionary<string, string> BuildClientToObjectIdMap()
        {
            if (Policies?.ManagedIdentity == null)
                return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            return Policies.ManagedIdentity
                .Where(p => !string.IsNullOrEmpty(p.ClientId) && !string.IsNullOrEmpty(p.ObjectId) && !string.Equals(p.ClientId, p.ObjectId, StringComparison.OrdinalIgnoreCase))
                .GroupBy(p => p.ClientId, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(
                    g => g.Key,
                    g => g.First().ObjectId,
                    StringComparer.OrdinalIgnoreCase);
        }

        private static void NormalizePrincipalList(List<AADObject> principals, Dictionary<string, string> clientToObject)
        {
            if (principals == null) return;

            foreach (var principal in principals)
            {
                if (string.IsNullOrEmpty(principal.Id)) continue;

                var (kind, guid, rest) = ParseFqn(principal.Id);
                if (kind == null || guid == null) continue;

                if (kind.Equals("aadapp", StringComparison.OrdinalIgnoreCase)
                    && clientToObject.TryGetValue(guid, out var objectId))
                {
                    principal.Id = $"{kind}={objectId}{rest}";
                }
            }
        }

        /// <summary>
        /// Parses a PrincipalFQN like "aadapp=guid;tenant" into (kind, guid, rest).
        /// rest includes the semicolon and tenant portion.
        /// </summary>
        internal static (string kind, string guid, string rest) ParseFqn(string fqn)
        {
            var eqIndex = fqn.IndexOf('=');
            if (eqIndex < 0) return (null, null, null);

            var kind = fqn[..eqIndex];
            var afterEq = fqn[(eqIndex + 1)..];

            var semiIndex = afterEq.IndexOf(';');
            if (semiIndex < 0) return (kind, afterEq, "");

            var guid = afterEq[..semiIndex];
            var rest = afterEq[semiIndex..];
            return (kind, guid, rest);
        }
    }
}
