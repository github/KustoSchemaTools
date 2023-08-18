using KustoSchemaTools.Changes;
using YamlDotNet.Serialization;

namespace KustoSchemaTools.Model
{
    public class Function : IKustoBaseEntity
    {
        public bool SkipValidation { get; set; } = false;
        public bool View { get; set; } = false;
        public string Folder { get; set; } = "";
        public string DocString { get; set; } = "";
        public string Parameters { get; set; } = "";
        [YamlMember(ScalarStyle = YamlDotNet.Core.ScalarStyle.Literal)]

        public string Body { get; set; }

        [YamlIgnore]
        public int Priority { get; set; } = 40;

        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            var properties = GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && p.Name != "Body" && p.Name != "Parameters")
                .Select(p => $"{p.Name}=\"{p.GetValue(this)}\"");
            var propertiesString = string.Join(", ", properties);
            return new List<DatabaseScriptContainer> { new DatabaseScriptContainer("CreateOrAlterFunction", Priority, $".create-or-alter function with({propertiesString}) {name} ({Parameters}) {{ {Body} }}") };
        }
    }

}
