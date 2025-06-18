using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class ClusterWorkloadGroup
    {
        public string Name { get; set; } = string.Empty;
        public int? RequestLimitsQueryCount { get; set; }
        public TimeSpan? RequestLimitsQueryCpuSecondsPerHour { get; set; }
        public TimeSpan? RequestLimitsQueryCpuSecondsPerDay { get; set; }
        public long? RequestLimitsQueryMemoryPerQueryInBytes { get; set; }
        public long? RequestLimitsQueryMemoryPerIteratorInBytes { get; set; }
        public TimeSpan? RequestLimitsQueryTimeoutPerQuery { get; set; }
        public long? RequestLimitsQueryResultSetSizeInBytes { get; set; }
        public int? RequestLimitsQueryResultRecordCount { get; set; }
        public int? RequestClassificationPolicyQueryWeightPercent { get; set; }
        public List<string>? RequestClassificationPolicyDatabases { get; set; }
        public List<string>? RequestClassificationPolicyPrincipals { get; set; }
        public Dictionary<string, object>? RequestClassificationPolicyQueryTexts { get; set; }

        public DatabaseScriptContainer CreateScript()
        {
            var workloadGroupObject = new Dictionary<string, object>();
            
            // Add request limits
            var requestLimits = new Dictionary<string, object>();
            if (RequestLimitsQueryCount.HasValue)
                requestLimits["QueryCount"] = RequestLimitsQueryCount.Value;
            if (RequestLimitsQueryCpuSecondsPerHour.HasValue)
                requestLimits["QueryCpuSecondsPerHour"] = (int)RequestLimitsQueryCpuSecondsPerHour.Value.TotalSeconds;
            if (RequestLimitsQueryCpuSecondsPerDay.HasValue)
                requestLimits["QueryCpuSecondsPerDay"] = (int)RequestLimitsQueryCpuSecondsPerDay.Value.TotalSeconds;
            if (RequestLimitsQueryMemoryPerQueryInBytes.HasValue)
                requestLimits["QueryMemoryPerQueryInBytes"] = RequestLimitsQueryMemoryPerQueryInBytes.Value;
            if (RequestLimitsQueryMemoryPerIteratorInBytes.HasValue)
                requestLimits["QueryMemoryPerIteratorInBytes"] = RequestLimitsQueryMemoryPerIteratorInBytes.Value;
            if (RequestLimitsQueryTimeoutPerQuery.HasValue)
                requestLimits["QueryTimeoutPerQuery"] = RequestLimitsQueryTimeoutPerQuery.Value;
            if (RequestLimitsQueryResultSetSizeInBytes.HasValue)
                requestLimits["QueryResultSetSizeInBytes"] = RequestLimitsQueryResultSetSizeInBytes.Value;
            if (RequestLimitsQueryResultRecordCount.HasValue)
                requestLimits["QueryResultRecordCount"] = RequestLimitsQueryResultRecordCount.Value;

            if (requestLimits.Any())
                workloadGroupObject["RequestLimits"] = requestLimits;

            // Add request classification policy
            var requestClassificationPolicy = new Dictionary<string, object>();
            if (RequestClassificationPolicyQueryWeightPercent.HasValue)
                requestClassificationPolicy["QueryWeightPercent"] = RequestClassificationPolicyQueryWeightPercent.Value;
            if (RequestClassificationPolicyDatabases?.Any() == true)
                requestClassificationPolicy["Databases"] = RequestClassificationPolicyDatabases;
            if (RequestClassificationPolicyPrincipals?.Any() == true)
                requestClassificationPolicy["Principals"] = RequestClassificationPolicyPrincipals;
            if (RequestClassificationPolicyQueryTexts?.Any() == true)
                requestClassificationPolicy["QueryTexts"] = RequestClassificationPolicyQueryTexts;

            if (requestClassificationPolicy.Any())
                workloadGroupObject["RequestClassificationPolicy"] = requestClassificationPolicy;

            var json = JsonConvert.SerializeObject(workloadGroupObject, Serialization.JsonPascalCase);
            var script = $".create-or-alter workload_group {Name} ```{json}```";
            
            return new DatabaseScriptContainer("ClusterWorkloadGroup", 30, script);
        }

        public DatabaseScriptContainer CreateDeletionScript()
        {
            var script = $".drop workload_group {Name}";
            return new DatabaseScriptContainer("ClusterWorkloadGroupDeletion", 30, script);
        }
    }
}
