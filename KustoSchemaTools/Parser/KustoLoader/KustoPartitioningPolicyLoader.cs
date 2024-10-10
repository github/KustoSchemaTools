using Kusto.Data.Common;
using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;

namespace KustoSchemaTools.Parser.KustoLoader
{
    public class KustoPartitioningPolicyLoader : IKustoBulkEntitiesLoader
    {
        const string query = """          
            .show database cslschema script 
            | project tostring(DatabaseSchemaScript)
            | where DatabaseSchemaScript contains 'policy partitioning'
            | extend parts = split(DatabaseSchemaScript, " ")
            | project Kind = tostring(parts[1]), Entity = tostring(parts[2]), Policy=parse_json(tostring(parse_json(tostring(parts[5]))))
            | mv-apply Policy.PartitionKeys on (
                project Policy_PartitionKeys, 
                        ColumName = tostring(Policy_PartitionKeys.ColumnName), 
                        Kind = toint(Policy_PartitionKeys.Kind), 
                        MaxPartitionCount = toint(Policy_PartitionKeys.Properties.MaxPartitionCount), 
                        PartitionAssignmentMode = toint(Policy_PartitionKeys.Properties.PartitionAssignmentMode),
                        Reference = todatetime(Policy_PartitionKeys.Properties.Reference), 
                        RangeSize = totimespan(Policy_PartitionKeys.Properties.RangeSize),
                        OverrideCreationTime = Policy_PartitionKeys.Properties.OverrideCreationTime
                | summarize TimePartitionColumn=take_anyif(ColumName, Kind==2), 
                            RangeSize=take_anyif(RangeSize, Kind==2),
                            OverrideCreationTime=tobool(take_anyif(OverrideCreationTime, Kind==2)),
                            Reference=take_anyif(Reference, Kind==2),
                            SecondaryPartition=take_anyif(ColumName, Kind==1),
                            PartitionAssignmentMode=take_anyif(PartitionAssignmentMode, Kind==1),
                            MaxPartitionCount=take_anyif(MaxPartitionCount, Kind==1)
            )
            | extend EffectiveDateTime = todatetime(Policy.EffectiveDateTime)
            | project EntityName = Entity, EntityType = Kind,  Body = bag_pack_columns(TimePartitionColumn, RangeSize, OverrideCreationTime, Reference, SecondaryPartition, PartitionAssignmentMode, MaxPartitionCount, EffectiveDateTime)            
            """;
        public async Task Load(Database database, string databaseName, KustoClient client)
        {
            var partitioningPoliciesQueryResult = await client.Client.ExecuteQueryAsync(databaseName, query, new ClientRequestProperties());
            var partitioningPolicies = partitioningPoliciesQueryResult.As<EnitityLoader<PartitioningPolicy>>().ToDictionary(itm => itm.EntityName, itm => itm.Body);

            foreach (var item in partitioningPolicies)
            {
                if (database.Tables.ContainsKey(item.Key))
                {
                    var entity = database.Tables[item.Key];
                    if (entity.Policies == null)
                    {
                        entity.Policies = new();
                    }
                    entity.Policies.Partitioning = item.Value;
                }
                if (database.MaterializedViews.ContainsKey(item.Key))
                {
                    var entity = database.MaterializedViews[item.Key];
                    if (entity.Policies == null)
                    {
                        entity.Policies = new();
                    }
                    entity.Policies.Partitioning = item.Value;
                }
            }
        }
    }
}
