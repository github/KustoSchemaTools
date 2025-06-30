using KustoSchemaTools.Changes;
using KustoSchemaTools.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using Kusto.Language;

namespace KustoSchemaTools.Model
{
    public class ClusterCapacityPolicy : IEquatable<ClusterCapacityPolicy>
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

        public bool Equals(ClusterCapacityPolicy? other)
        {
            if (other is null) return false;
            if (ReferenceEquals(this, other)) return true;
            return
                EqualityComparer<IngestionCapacity?>.Default.Equals(IngestionCapacity, other.IngestionCapacity) &&
                EqualityComparer<ExtentsMergeCapacity?>.Default.Equals(ExtentsMergeCapacity, other.ExtentsMergeCapacity) &&
                EqualityComparer<ExtentsPurgeRebuildCapacity?>.Default.Equals(ExtentsPurgeRebuildCapacity, other.ExtentsPurgeRebuildCapacity) &&
                EqualityComparer<ExportCapacity?>.Default.Equals(ExportCapacity, other.ExportCapacity) &&
                EqualityComparer<ExtentsPartitionCapacity?>.Default.Equals(ExtentsPartitionCapacity, other.ExtentsPartitionCapacity) &&
                EqualityComparer<MaterializedViewsCapacity?>.Default.Equals(MaterializedViewsCapacity, other.MaterializedViewsCapacity) &&
                EqualityComparer<StoredQueryResultsCapacity?>.Default.Equals(StoredQueryResultsCapacity, other.StoredQueryResultsCapacity) &&
                EqualityComparer<StreamingIngestionPostProcessingCapacity?>.Default.Equals(StreamingIngestionPostProcessingCapacity, other.StreamingIngestionPostProcessingCapacity) &&
                EqualityComparer<PurgeStorageArtifactsCleanupCapacity?>.Default.Equals(PurgeStorageArtifactsCleanupCapacity, other.PurgeStorageArtifactsCleanupCapacity) &&
                EqualityComparer<PeriodicStorageArtifactsCleanupCapacity?>.Default.Equals(PeriodicStorageArtifactsCleanupCapacity, other.PeriodicStorageArtifactsCleanupCapacity) &&
                EqualityComparer<QueryAccelerationCapacity?>.Default.Equals(QueryAccelerationCapacity, other.QueryAccelerationCapacity) &&
                EqualityComparer<GraphSnapshotsCapacity?>.Default.Equals(GraphSnapshotsCapacity, other.GraphSnapshotsCapacity);
        }

        public override bool Equals(object? obj) => Equals(obj as ClusterCapacityPolicy);
        public override int GetHashCode()
        {
            var hc = new HashCode();
            hc.Add(IngestionCapacity);
            hc.Add(ExtentsMergeCapacity);
            hc.Add(ExtentsPurgeRebuildCapacity);
            hc.Add(ExportCapacity);
            hc.Add(ExtentsPartitionCapacity);
            hc.Add(MaterializedViewsCapacity);
            hc.Add(StoredQueryResultsCapacity);
            hc.Add(StreamingIngestionPostProcessingCapacity);
            hc.Add(PurgeStorageArtifactsCleanupCapacity);
            hc.Add(PeriodicStorageArtifactsCleanupCapacity);
            hc.Add(QueryAccelerationCapacity);
            hc.Add(GraphSnapshotsCapacity);
            return hc.ToHashCode();
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            });
        }

        public string ToUpdateScript()
        {
            var json = ToJson();
            var script = $".alter-merge cluster policy capacity ```{json}```";
            var parsedScript = KustoCode.Parse(script);
            var diagnostics = parsedScript.GetDiagnostics();
            if (diagnostics.Any())
            {
                Console.WriteLine($"Generated script: {diagnostics[0]}");

            }
            return script;
        }
    }

    public class IngestionCapacity : IEquatable<IngestionCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }

        public bool Equals(IngestionCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations &&
                   CoreUtilizationCoefficient == other.CoreUtilizationCoefficient;
        }
        public override bool Equals(object? obj) => Equals(obj as IngestionCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations, CoreUtilizationCoefficient);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class ExtentsMergeCapacity : IEquatable<ExtentsMergeCapacity>
    {
        public int? MinimumConcurrentOperationsPerNode { get; set; }
        public int? MaximumConcurrentOperationsPerNode { get; set; }

        public bool Equals(ExtentsMergeCapacity? other)
        {
            if (other is null) return false;
            return MinimumConcurrentOperationsPerNode == other.MinimumConcurrentOperationsPerNode &&
                   MaximumConcurrentOperationsPerNode == other.MaximumConcurrentOperationsPerNode;
        }
        public override bool Equals(object? obj) => Equals(obj as ExtentsMergeCapacity);
        public override int GetHashCode() => HashCode.Combine(MinimumConcurrentOperationsPerNode, MaximumConcurrentOperationsPerNode);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class ExtentsPurgeRebuildCapacity : IEquatable<ExtentsPurgeRebuildCapacity>
    {
        public int? MaximumConcurrentOperationsPerNode { get; set; }

        public bool Equals(ExtentsPurgeRebuildCapacity? other)
        {
            if (other is null) return false;
            return MaximumConcurrentOperationsPerNode == other.MaximumConcurrentOperationsPerNode;
        }
        public override bool Equals(object? obj) => Equals(obj as ExtentsPurgeRebuildCapacity);
        public override int GetHashCode() => HashCode.Combine(MaximumConcurrentOperationsPerNode);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class ExportCapacity : IEquatable<ExportCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }

        public bool Equals(ExportCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations &&
                   CoreUtilizationCoefficient == other.CoreUtilizationCoefficient;
        }
        public override bool Equals(object? obj) => Equals(obj as ExportCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations, CoreUtilizationCoefficient);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class ExtentsPartitionCapacity : IEquatable<ExtentsPartitionCapacity>
    {
        public int? ClusterMinimumConcurrentOperations { get; set; }
        public int? ClusterMaximumConcurrentOperations { get; set; }

        public bool Equals(ExtentsPartitionCapacity? other)
        {
            if (other is null) return false;
            return ClusterMinimumConcurrentOperations == other.ClusterMinimumConcurrentOperations &&
                   ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations;
        }
        public override bool Equals(object? obj) => Equals(obj as ExtentsPartitionCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMinimumConcurrentOperations, ClusterMaximumConcurrentOperations);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class MaterializedViewsCapacity : IEquatable<MaterializedViewsCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public ExtentsRebuildCapacity? ExtentsRebuildCapacity { get; set; }

        public bool Equals(MaterializedViewsCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations &&
                   EqualityComparer<ExtentsRebuildCapacity?>.Default.Equals(ExtentsRebuildCapacity, other.ExtentsRebuildCapacity);
        }
        public override bool Equals(object? obj) => Equals(obj as MaterializedViewsCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations, ExtentsRebuildCapacity);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class ExtentsRebuildCapacity : IEquatable<ExtentsRebuildCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public int? MaximumConcurrentOperationsPerNode { get; set; }

        public bool Equals(ExtentsRebuildCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations &&
                   MaximumConcurrentOperationsPerNode == other.MaximumConcurrentOperationsPerNode;
        }
        public override bool Equals(object? obj) => Equals(obj as ExtentsRebuildCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations, MaximumConcurrentOperationsPerNode);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class StoredQueryResultsCapacity : IEquatable<StoredQueryResultsCapacity>
    {
        public int? MaximumConcurrentOperationsPerDbAdmin { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }

        public bool Equals(StoredQueryResultsCapacity? other)
        {
            if (other is null) return false;
            return MaximumConcurrentOperationsPerDbAdmin == other.MaximumConcurrentOperationsPerDbAdmin &&
                   CoreUtilizationCoefficient == other.CoreUtilizationCoefficient;
        }
        public override bool Equals(object? obj) => Equals(obj as StoredQueryResultsCapacity);
        public override int GetHashCode() => HashCode.Combine(MaximumConcurrentOperationsPerDbAdmin, CoreUtilizationCoefficient);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class StreamingIngestionPostProcessingCapacity : IEquatable<StreamingIngestionPostProcessingCapacity>
    {
        public int? MaximumConcurrentOperationsPerNode { get; set; }

        public bool Equals(StreamingIngestionPostProcessingCapacity? other)
        {
            if (other is null) return false;
            return MaximumConcurrentOperationsPerNode == other.MaximumConcurrentOperationsPerNode;
        }
        public override bool Equals(object? obj) => Equals(obj as StreamingIngestionPostProcessingCapacity);
        public override int GetHashCode() => HashCode.Combine(MaximumConcurrentOperationsPerNode);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class PurgeStorageArtifactsCleanupCapacity : IEquatable<PurgeStorageArtifactsCleanupCapacity>
    {
        public int? MaximumConcurrentOperationsPerCluster { get; set; }

        public bool Equals(PurgeStorageArtifactsCleanupCapacity? other)
        {
            if (other is null) return false;
            return MaximumConcurrentOperationsPerCluster == other.MaximumConcurrentOperationsPerCluster;
        }
        public override bool Equals(object? obj) => Equals(obj as PurgeStorageArtifactsCleanupCapacity);
        public override int GetHashCode() => HashCode.Combine(MaximumConcurrentOperationsPerCluster);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class PeriodicStorageArtifactsCleanupCapacity : IEquatable<PeriodicStorageArtifactsCleanupCapacity>
    {
        public int? MaximumConcurrentOperationsPerCluster { get; set; }

        public bool Equals(PeriodicStorageArtifactsCleanupCapacity? other)
        {
            if (other is null) return false;
            return MaximumConcurrentOperationsPerCluster == other.MaximumConcurrentOperationsPerCluster;
        }
        public override bool Equals(object? obj) => Equals(obj as PeriodicStorageArtifactsCleanupCapacity);
        public override int GetHashCode() => HashCode.Combine(MaximumConcurrentOperationsPerCluster);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class QueryAccelerationCapacity : IEquatable<QueryAccelerationCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }
        public double? CoreUtilizationCoefficient { get; set; }

        public bool Equals(QueryAccelerationCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations &&
                   CoreUtilizationCoefficient == other.CoreUtilizationCoefficient;
        }
        public override bool Equals(object? obj) => Equals(obj as QueryAccelerationCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations, CoreUtilizationCoefficient);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }

    public class GraphSnapshotsCapacity : IEquatable<GraphSnapshotsCapacity>
    {
        public int? ClusterMaximumConcurrentOperations { get; set; }

        public bool Equals(GraphSnapshotsCapacity? other)
        {
            if (other is null) return false;
            return ClusterMaximumConcurrentOperations == other.ClusterMaximumConcurrentOperations;
        }
        public override bool Equals(object? obj) => Equals(obj as GraphSnapshotsCapacity);
        public override int GetHashCode() => HashCode.Combine(ClusterMaximumConcurrentOperations);
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this, new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.None
            });
        }
    }
}