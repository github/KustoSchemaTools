using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class ClusterPolicy
    {
        // Query Weak Consistency Policy
        public bool? QueryWeakConsistencyEnabled { get; set; }
        public int? QueryWeakConsistencyMaxPercentage { get; set; }

        // Query Throttling Policy
        public int? QueryThrottlingConcurrentQueriesLimit { get; set; }
        public int? QueryThrottlingConcurrentHeavyQueriesLimit { get; set; }

        // Callout Policy
        public bool? CalloutPolicyEnabled { get; set; }
        public List<string>? CalloutPolicyAllowedDomains { get; set; }
        public List<string>? CalloutPolicyBlockedDomains { get; set; }

        // Sandboxing Policy
        public bool? SandboxingPolicyEnabled { get; set; }
        public Dictionary<string, object>? SandboxingPolicySettings { get; set; }

        // Capacity Policy
        public int? CapacityPolicyClusterMaximumConcurrentOperations { get; set; }
        public int? CapacityPolicyClusterMaximumIngestConcurrentOperations { get; set; }
        public int? CapacityPolicyClusterMaximumExportConcurrentOperations { get; set; }

        // Streaming Ingestion Policy
        public bool? StreamingIngestionPolicyEnabled { get; set; }
        public TimeSpan? StreamingIngestionPolicyHintAllocatedRate { get; set; }

        // Multi-Database Administrators Policy
        public List<string>? MultiDatabaseAdministrators { get; set; }

        // Request Classification Policy
        public bool? RequestClassificationPolicyEnabled { get; set; }
        public List<Dictionary<string, object>>? RequestClassificationPolicyRules { get; set; }

        public List<DatabaseScriptContainer> CreateScripts()
        {
            var scripts = new List<DatabaseScriptContainer>();

            // Query Weak Consistency Policy
            if (QueryWeakConsistencyEnabled.HasValue || QueryWeakConsistencyMaxPercentage.HasValue)
            {
                var policy = new Dictionary<string, object>();
                if (QueryWeakConsistencyEnabled.HasValue)
                    policy["IsEnabled"] = QueryWeakConsistencyEnabled.Value;
                if (QueryWeakConsistencyMaxPercentage.HasValue)
                    policy["MaximumLagAllowedInMinutes"] = QueryWeakConsistencyMaxPercentage.Value;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("QueryWeakConsistency", 20, $".alter cluster policy query_weak_consistency ```{json}```"));
            }

            // Query Throttling Policy
            if (QueryThrottlingConcurrentQueriesLimit.HasValue || QueryThrottlingConcurrentHeavyQueriesLimit.HasValue)
            {
                var policy = new Dictionary<string, object>();
                if (QueryThrottlingConcurrentQueriesLimit.HasValue)
                    policy["MaxConcurrentQueries"] = QueryThrottlingConcurrentQueriesLimit.Value;
                if (QueryThrottlingConcurrentHeavyQueriesLimit.HasValue)
                    policy["MaxConcurrentHeavyQueries"] = QueryThrottlingConcurrentHeavyQueriesLimit.Value;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("QueryThrottling", 21, $".alter cluster policy query_throttling ```{json}```"));
            }

            // Callout Policy
            if (CalloutPolicyEnabled.HasValue || CalloutPolicyAllowedDomains?.Any() == true || CalloutPolicyBlockedDomains?.Any() == true)
            {
                var policy = new Dictionary<string, object>();
                if (CalloutPolicyEnabled.HasValue)
                    policy["IsEnabled"] = CalloutPolicyEnabled.Value;
                if (CalloutPolicyAllowedDomains?.Any() == true)
                    policy["AllowedDomains"] = CalloutPolicyAllowedDomains;
                if (CalloutPolicyBlockedDomains?.Any() == true)
                    policy["BlockedDomains"] = CalloutPolicyBlockedDomains;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("Callout", 22, $".alter cluster policy callout ```{json}```"));
            }

            // Sandboxing Policy
            if (SandboxingPolicyEnabled.HasValue || SandboxingPolicySettings?.Any() == true)
            {
                var policy = new Dictionary<string, object>();
                if (SandboxingPolicyEnabled.HasValue)
                    policy["IsEnabled"] = SandboxingPolicyEnabled.Value;
                if (SandboxingPolicySettings?.Any() == true)
                    policy["Settings"] = SandboxingPolicySettings;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("Sandboxing", 23, $".alter cluster policy sandboxing ```{json}```"));
            }

            // Capacity Policy
            if (CapacityPolicyClusterMaximumConcurrentOperations.HasValue || 
                CapacityPolicyClusterMaximumIngestConcurrentOperations.HasValue ||
                CapacityPolicyClusterMaximumExportConcurrentOperations.HasValue)
            {
                var policy = new Dictionary<string, object>();
                if (CapacityPolicyClusterMaximumConcurrentOperations.HasValue)
                    policy["ClusterMaximumConcurrentOperations"] = CapacityPolicyClusterMaximumConcurrentOperations.Value;
                if (CapacityPolicyClusterMaximumIngestConcurrentOperations.HasValue)
                    policy["ClusterMaximumIngestConcurrentOperations"] = CapacityPolicyClusterMaximumIngestConcurrentOperations.Value;
                if (CapacityPolicyClusterMaximumExportConcurrentOperations.HasValue)
                    policy["ClusterMaximumExportConcurrentOperations"] = CapacityPolicyClusterMaximumExportConcurrentOperations.Value;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("Capacity", 24, $".alter cluster policy capacity ```{json}```"));
            }

            // Streaming Ingestion Policy
            if (StreamingIngestionPolicyEnabled.HasValue || StreamingIngestionPolicyHintAllocatedRate.HasValue)
            {
                var policy = new Dictionary<string, object>();
                if (StreamingIngestionPolicyEnabled.HasValue)
                    policy["IsEnabled"] = StreamingIngestionPolicyEnabled.Value;
                if (StreamingIngestionPolicyHintAllocatedRate.HasValue)
                    policy["HintAllocatedRate"] = StreamingIngestionPolicyHintAllocatedRate.Value;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("StreamingIngestion", 25, $".alter cluster policy streamingingestion ```{json}```"));
            }

            // Multi-Database Administrators Policy
            if (MultiDatabaseAdministrators?.Any() == true)
            {
                var principals = string.Join(", ", MultiDatabaseAdministrators.Select(p => $"aaduser={p}"));
                scripts.Add(new DatabaseScriptContainer("MultiDatabaseAdministrators", 26, $".add cluster admins ({principals})"));
            }

            // Request Classification Policy
            if (RequestClassificationPolicyEnabled.HasValue || RequestClassificationPolicyRules?.Any() == true)
            {
                var policy = new Dictionary<string, object>();
                if (RequestClassificationPolicyEnabled.HasValue)
                    policy["IsEnabled"] = RequestClassificationPolicyEnabled.Value;
                if (RequestClassificationPolicyRules?.Any() == true)
                    policy["Rules"] = RequestClassificationPolicyRules;

                var json = JsonConvert.SerializeObject(policy, Serialization.JsonPascalCase);
                scripts.Add(new DatabaseScriptContainer("RequestClassification", 27, $".alter cluster policy request_classification ```{json}```"));
            }

            return scripts;
        }
    }
}
