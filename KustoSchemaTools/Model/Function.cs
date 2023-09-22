using KustoSchemaTools.Changes;
using KustoSchemaTools.Parser;
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

        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            var properties = GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && p.Name != "Body" && p.Name != "Parameters")
                .Select(p => $"{p.Name}=\"{p.GetValue(this)}\"");
            var propertiesString = string.Join(", ", properties);

            var parameters = string.IsNullOrEmpty(Parameters) 
                ? Parameters 
                : string.Join(',', Parameters.Split(',').Select(p => p.Split(':')).Select(itm => $"{itm[0].Trim().BracketIfIdentifier()}:{itm[1]}"));

            return new List<DatabaseScriptContainer> { new DatabaseScriptContainer("CreateOrAlterFunction", 40, $".create-or-alter function with({propertiesString}) {name} ({parameters}) {{ {Body} }}") };
        }
    }

}
