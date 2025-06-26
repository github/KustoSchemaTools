using KustoSchemaTools.Model;
using System.Collections.Generic;
using System.Linq;

namespace KustoSchemaTools.Changes
{
    /// <summary>
    /// Represents the complete set of changes for a single Kusto cluster,
    /// including the old and new state, and a list of policy modifications.
    /// </summary>
    public class ClusterChangeSet : BaseChange<Cluster>
    {
        /// <summary>
        /// A list of specific, granular changes detected for the cluster's policies.
        /// Each item is typically a BaseChange&lt;T&gt; for a specific policy type.
        /// </summary>
        public List<IChange> Changes { get; } = new List<IChange>();

        public ClusterChangeSet(string clusterName, Cluster from, Cluster to)
            : base("Cluster", clusterName, from, to)
        {
            // Consolidate all scripts from policy changes into this top-level object
            Scripts.AddRange(Changes.SelectMany(c => c.Scripts));
        }
    }
}