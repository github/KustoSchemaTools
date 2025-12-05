using Newtonsoft.Json;

namespace KustoSchemaTools.Model
{
    public class DatabaseScript
    {
        public DatabaseScript(string text, int order)
        {
            Text = text;
            Order = order;
        }

        public DatabaseScript()
        {
        }

        [JsonProperty("text")]
        public string Text { get; set; }

        [JsonProperty("order")]
        public int Order { get; set; }
    }

}
