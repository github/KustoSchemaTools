using KustoSchemaTools.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class FollowerLoader
    {

        const string FollowerMetadataQuery = @".show follower database {0}
| extend  TableMetadataOverridesJson=parse_json(TableMetadataOverrides)
| mv-apply TableMetadataOverridesJson on(
     project Table=tostring(bag_keys(TableMetadataOverridesJson)[0]), TableMetadataOverridesJson, Fragments=split(TableMetadataOverridesJson, '""')
    | mv-apply Fragments on (
        project Timespan=totimespan(Fragments)
        | where isnotempty(Timespan)
        | limit 1
    )
| summarize CachingPolicies=make_bag(bag_pack(Table,Timespan))
)
";

        public static FollowerDatabase LoadFollower(string databaseName, KustoClient client)
        {
            var follower = new FollowerDatabase { DatabaseName = databaseName };
            // Execute the query and handle the case where no rows are returned (e.g., database is not a follower)
            var queryResult = client.Client.ExecuteQuery(string.Format(FollowerMetadataQuery, databaseName));
            var metdaData = queryResult.As<FollowerMetadata>().FirstOrDefault();

            if (metdaData == null)
            {
                // No follower metadata found; return a default follower object (no changes will be generated)
                return follower;
            }

            switch (metdaData.AuthorizedPrincipalsModificationKind)
            {
                case "Union":
                    follower.Permissions.ModificationKind = FollowerModificationKind.Union;
                    break;
                case "Replace":
                    follower.Permissions.ModificationKind = FollowerModificationKind.Replace;
                    break;
                default:
                    follower.Permissions.ModificationKind = FollowerModificationKind.None;
                    break;
            }

            switch (metdaData.CachingPoliciesModificationKind)
            {
                case "Union":
                    follower.Cache.ModificationKind = FollowerModificationKind.Union;
                    break;
                case "Replace":
                    follower.Cache.ModificationKind = FollowerModificationKind.Replace;
                    break;
                default:
                    follower.Cache.ModificationKind = FollowerModificationKind.None;
                    break;
            }

            foreach (var kvp in metdaData.CachingPolicies)
            {
                var isMv = kvp.Key.StartsWith("_MV_");
                var key = isMv ? kvp.Key.Substring(4): kvp.Key;

                var target = isMv? follower.Cache.MaterializedViews : follower.Cache.Tables;
                target.Add(key, kvp.Value.Days+"d");
            }

            return follower;
        }
    }

    public class FollowerMetadata
    {        
        public string? DatabaseName { get; set; }
        public string? LeaderClusterMetadataPath { get; set; }
        public string? CachingPolicyOverride { get; set; }
        public string? AuthorizedPrincipalsOverride { get; set; }
        public string? AuthorizedPrincipalsModificationKind { get; set; }
        public bool IsAutoPrefetchEnabled { get; set; }
        public string? TableMetadataOverrides { get; set; }
        public string? CachingPoliciesModificationKind { get; set; }
        public string? ChildEntities { get; set; }
        public string? OriginalDatabaseName { get; set; }
        public Dictionary<string,TimeSpan> CachingPolicies { get; set; } = new Dictionary<string, TimeSpan>();

    }
}
