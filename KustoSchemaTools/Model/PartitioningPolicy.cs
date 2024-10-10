using KustoSchemaTools.Changes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace KustoSchemaTools.Model
{
    public class PartitioningPolicy
    {
        public string TimePartitionColumn { get; set; }
        public TimeSpan? RangeSize { get; set; }
        public bool? OverrideCreationTime { get; set; }
        public DateTime? Reference { get; set; }

        public string? SecondaryPartition { get; set; }
        public PartitionAssignmentMode? PartirionAssignmentMode { get; set; }
        public int? MaxPartitionCount { get; set; }
        public DateTime? EffectiveDateTime { get; set; }

        public DatabaseScriptContainer CreateScript(string name, string entity)
        {

            var p1 = new
            {
                ColumnName = TimePartitionColumn,
                Kind = "UniformRange",
                Properties = new
                {
                    Reference = (Reference ?? DateTime.UtcNow).ToString("yyyy-MM-ddTHH:mm:ss"),
                    RangeSize = (RangeSize ?? new TimeSpan(1, 0, 0, 0)).ToString(),
                    OverrideCreationTime = OverrideCreationTime == true
                }
            };

            var p2 = new
            {
                ColumnName = SecondaryPartition,
                Kind = "Hash",
                Properties = new 
                {
                    Function = "XxHash64",
                    MaxPartitionCount = MaxPartitionCount ?? 128,
                    PartitionAssignmentMode = PartirionAssignmentMode ?? PartitionAssignmentMode.Default
                }
            };

            var items = new List<object>();
            items.Add(p1);
            if(string.IsNullOrWhiteSpace(SecondaryPartition) == false)
            {
                items.Add(p2);
            }

            var policy = new
            {
                EffectiveDateTime = (EffectiveDateTime ?? DateTime.UtcNow).ToString("yyyy-MM-dd"),
                PartitionKeys = items
            };
                   

            return new DatabaseScriptContainer("PartitioningPolicy", 50, $".alter {entity} {name} policy partitioning ```{JsonConvert.SerializeObject(policy, Formatting.None)}```");
        }

        public enum PartitionAssignmentMode
        {
            /// <summary>
            /// The default assignment mode.
            /// </summary>
            Default = 0,

            /// <summary>
            /// Ignore the partition value when assigning a homogeneous extent, and assign them uniformly according to the extent ID.
            /// </summary>
            Uniform = 1,

            /// <summary>
            /// Assign homogeneous (partitioned) extents by their partition value, this means assignment ignores extent ID.
            /// </summary>
            ByPartition = 2,
        }
    }
}
