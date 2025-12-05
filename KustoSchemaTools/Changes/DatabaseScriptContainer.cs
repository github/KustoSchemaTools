using KustoSchemaTools.Model;
using Newtonsoft.Json;

namespace KustoSchemaTools.Changes
{
    public class DatabaseScriptContainer
    {
        public DatabaseScriptContainer()
        {
            
        }

        public DatabaseScriptContainer(DatabaseScript script, string kind, bool isAsync = false)
        {
            Script = script;
            Kind = kind;
            IsAsync = isAsync;
        }

        public DatabaseScriptContainer(string kind, int order, string script, bool isAsync = false)
        {
            Script = new DatabaseScript(script, order);
            Kind = kind;
            IsAsync = isAsync;
        }

        [JsonProperty("script")]
        public DatabaseScript Script { get; set; }

        [JsonProperty("kind")]
        public string Kind { get; set; }

        [JsonProperty("isValid")]
        public bool? IsValid { get; set; }

        [JsonProperty("text")]
        public string Text => Script.Text;

        [JsonProperty("order")]
        public int Order => Script.Order;

        [JsonProperty("isAsync")]
        public bool IsAsync { get; set; }
    }
}
