using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
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
| summarize CachingPolicies=make_bag(bag_pack(Table,Timespan)),
            DatabaseName=any(DatabaseName),
            LeaderClusterMetadataPath=any(LeaderClusterMetadataPath),
            CachingPolicyOverride=any(CachingPolicyOverride),
            AuthorizedPrincipalsOverride=any(AuthorizedPrincipalsOverride),
            AuthorizedPrincipalsModificationKind=any(AuthorizedPrincipalsModificationKind),
            CachingPoliciesModificationKind=any(CachingPoliciesModificationKind),
            ChildEntities=any(ChildEntities),
            OriginalDatabaseName=any(OriginalDatabaseName),
            IsAutoPrefetchEnabled=any(IsAutoPrefetchEnabled),
            LeaderName=any(LeaderName)
)";

        public static FollowerDatabase LoadFollower(string databaseName, KustoClient client)
        {
            var follower = new FollowerDatabase { DatabaseName = databaseName };
            // Execute the query and handle the case where no rows are returned (e.g., database is not a follower)
            var queryResult = client.Client.ExecuteQuery(string.Format(FollowerMetadataQuery, databaseName.BracketIfIdentifier()));
            var metdaData = queryResult.As<FollowerMetadata>().FirstOrDefault();

            if (metdaData == null)
            {
                // No follower metadata found; return a default follower object (no changes will be generated)
                return follower;
            }

            follower.IsFollower = true;

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

            follower.Permissions.LeaderName = metdaData.LeaderName;

            if (!string.IsNullOrWhiteSpace(metdaData.AuthorizedPrincipalsOverride))
            {
                try
                {
                    var arr = JArray.Parse(metdaData.AuthorizedPrincipalsOverride);
                    foreach (var principalObj in arr)
                    {
                        var role = principalObj["Role"]?.Value<int?>();
                        var principal = principalObj["Principal"]?["FullyQualifiedName"]?.Value<string>()
                                        ?? principalObj["Principal"]?["Id"]?.Value<string>();
                        var displayName = principalObj["Principal"]?["DisplayName"]?.Value<string>()
                                         ?? principal;

                        if (string.IsNullOrWhiteSpace(principal) || role == null)
                        {
                            continue;
                        }

                        var aadObj = new AADObject
                        {
                            Id = principal,
                            Name = displayName
                        };

                        if (role == 0)
                        {
                            follower.Permissions.Admins.Add(aadObj);
                        }
                        else if (role == 2)
                        {
                            follower.Permissions.Viewers.Add(aadObj);
                        }
                    }
                }
                catch (Exception)
                {
                    // Ignore parse errors; treat as empty and skip permission diffs
                }
            }

            return follower;
        }
    }

    public class FollowerMetadata
    {        
        public string? DatabaseName { get; set; }
        public string? LeaderClusterMetadataPath { get; set; }
        public string? LeaderName { get; set; }
        public string? CachingPolicyOverride { get; set; }
        public string? AuthorizedPrincipalsOverride { get; set; }
        public string? AuthorizedPrincipalsModificationKind { get; set; }
        public bool IsAutoPrefetchEnabled { get; set; }
        public string? TableMetadataOverrides { get; set; }
        public string? CachingPoliciesModificationKind { get; set; }
        public string? ChildEntities { get; set; }
        public string? OriginalDatabaseName { get; set; }
        public Dictionary<string,TimeSpan>? CachingPolicies { get; set; }

    }
}
