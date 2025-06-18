using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Model
{
    public class Cluster
    {
        public string Name { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();
        public List<ClusterWorkloadGroup> WorkloadGroups { get; set; } = new List<ClusterWorkloadGroup>();
        public ClusterPolicy? Policy { get; set; }

        /// <summary>
        /// Generates all scripts for the cluster including workload groups and policies
        /// </summary>
        /// <returns>List of database script containers</returns>
        public List<DatabaseScriptContainer> GenerateAllScripts()
        {
            var scripts = new List<DatabaseScriptContainer>();

            // Add cluster policy scripts
            if (Policy != null)
            {
                scripts.AddRange(Policy.CreateScripts());
            }

            // Add workload group scripts
            foreach (var workloadGroup in WorkloadGroups)
            {
                scripts.Add(workloadGroup.CreateScript());
            }

            // Convert existing scripts to DatabaseScriptContainer format
            foreach (var script in Scripts)
            {
                scripts.Add(new DatabaseScriptContainer("ClusterScript", script.Order, script.Text));
            }

            return scripts.OrderBy(s => s.Order).ToList();
        }

        /// <summary>
        /// Generates deletion scripts for workload groups
        /// </summary>
        /// <param name="workloadGroupsToDelete">Names of workload groups to delete</param>
        /// <returns>List of deletion scripts</returns>
        public List<DatabaseScriptContainer> GenerateDeletionScripts(List<string> workloadGroupsToDelete)
        {
            var scripts = new List<DatabaseScriptContainer>();

            foreach (var workloadGroupName in workloadGroupsToDelete)
            {
                var workloadGroup = new ClusterWorkloadGroup { Name = workloadGroupName };
                scripts.Add(workloadGroup.CreateDeletionScript());
            }

            return scripts;
        }
    }
}
