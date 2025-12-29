using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class ScriptDiagnostic
    {
        [JsonProperty("start")]
        public int Start { get; set; }

        [JsonProperty("end")]
        public int End { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; } = string.Empty;
    }
}
