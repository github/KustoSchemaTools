using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class ClusterCapacityPolicy
    {
        public IngestionCapacity? IngestionCapacity { get; set; }
        public ExtentsMergeCapacity? ExtentsMergeCapacity { get; set; }
        public ExtentsPurgeRebuildCapacity? ExtentsPurgeRebuildCapacity { get; set; }
        public ExportCapacity? ExportCapacity { get; set; }
        public ExtentsPartitionCapacity? ExtentsPartitionCapacity { get; set; }
        public MaterializedViewsCapacity? MaterializedViewsCapacity { get; set; }
        public StoredQueryResultsCapacity? StoredQueryResultsCapacity { get; set; }
        public StreamingIngestionPostProcessingCapacity? StreamingIngestionPostProcessingCapacity { get; set; }
        public PurgeStorageArtifactsCleanupCapacity? PurgeStorageArtifactsCleanupCapacity { get; set; }
        public PeriodicStorageArtifactsCleanupCapacity? PeriodicStorageArtifactsCleanupCapacity { get; set; }
        public QueryAccelerationCapacity? QueryAccelerationCapacity { get; set; }
        public GraphSnapshotsCapacity? GraphSnapshotsCapacity { get; set; }

        public DatabaseScriptContainer CreateScript()
        {
            var policy = JsonConvert.SerializeObject(this, Serialization.JsonPascalCase);
            return new DatabaseScriptContainer("ClusterCapacityPolicy", 10, $".alter-merge cluster policy capacity ```{policy}```");
        }
    }

    public class IngestionCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }
    }

    public class ExtentsMergeCapacity
    {
        public int? MinimumConcurrentOperationsPerNode { get; set; }
        public int? MaximumConcurrentOperationsPerNode { get; set; }
    }

    public class ExtentsPurgeRebuildCapacity
    {
        public int? MaximumConcurrentOperationsPerNode { get; set; }
    }

    public class ExportCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }
    }

    public class ExtentsPartitionCapacity
    {
        public int? ClusterMinimumConcurrentOperations { get; set; }
        public int? ClusterMaximumConcurrentOperations { get; set; }
    }

    public class MaterializedViewsCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public ExtentsRebuildCapacity? ExtentsRebuildCapacity { get; set; }
    }

    public class ExtentsRebuildCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public int? MaximumConcurrentOperationsPerNode { get; set; }
    }

    public class StoredQueryResultsCapacity
    {
        public int? MaximumConcurrentOperationsPerDbAdmin { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }
    }

    public class StreamingIngestionPostProcessingCapacity
    {
        public int? MaximumConcurrentOperationsPerNode { get; set; }
    }

    public class PurgeStorageArtifactsCleanupCapacity
    {
        public int? MaximumConcurrentOperationsPerCluster { get; set; }
    }

    public class PeriodicStorageArtifactsCleanupCapacity
    {
        public int? MaximumConcurrentOperationsPerCluster { get; set; }
    }

    public class QueryAccelerationCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }
    }

    public class GraphSnapshotsCapacity
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
    }
}
