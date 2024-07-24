using KustoSchemaTools.Changes;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class ContinuousExport : IKustoBaseEntity
    {
        public string ExternalTable { get; set; }
        public int ForcedLatencyInMinutes { get; set; }
        public int IntervalBetweenRuns { get; set; }
        public long SizeLimit { get; set; }
        public bool Distributed { get; set; }
        public string ManagedIdentity { get; set; }
        [YamlMember(ScalarStyle = YamlDotNet.Core.ScalarStyle.Literal)]
        public string Query { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            return new List<DatabaseScriptContainer>
            {
                new DatabaseScriptContainer("ContinuousExport",120,@$".create-or-alter continuous-export {name} to table {ExternalTable} with (forcedLatency={ForcedLatencyInMinutes}m, intervalBetweenRuns={ForcedLatencyInMinutes}m, sizeLimit={SizeLimit}, distributed={Distributed}, managedIdentity='{ManagedIdentity}') <| {Query}")
            };
        }
    }
}