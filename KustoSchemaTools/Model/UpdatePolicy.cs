using KustoSchemaTools.Helpers;
using Newtonsoft.Json;
using System.ComponentModel;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class UpdatePolicy
    {
        [DefaultValue(true)]
        public bool IsEnabled { get; set; } = true;
        [DefaultValue(true)]
        public bool PropagateIngestionProperties { get; set; } = true;
        public string Source { get; set; }
        [YamlMember(ScalarStyle = YamlDotNet.Core.ScalarStyle.Literal)]
        public string Query { get; set; }
        public bool IsTransactional { get; set; } = false;
        public string ManagedIdentity { get; set; }


        public string CreateScript(string name)
        {
            return JsonConvert.SerializeObject(this, Serialization.JsonPascalCase);
        }
    }

}
