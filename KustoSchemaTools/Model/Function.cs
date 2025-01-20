using Kusto.Language.Syntax;
using Kusto.Language;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Parser;
using System.Text;
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

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            var properties = GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && p.Name != "Body" && p.Name != "Parameters")
                .Select(p => $"{p.Name}=```{p.GetValue(this)}```");
            var propertiesString = string.Join(", ", properties);

            var parameters = Parameters;
            if (!string.IsNullOrWhiteSpace(Parameters))
            {
                var dummyFunction = $"let x = ({parameters}) {{print \"abc\"}}";
                var parsed = KustoCode.Parse(dummyFunction);

                var descs = parsed.Syntax
                    .GetDescendants<FunctionParameters>()
                    .First()
                    .GetDescendants<NameDeclaration>()
                    .ToList();

                var sb = new StringBuilder();
                int lastPos = 0;
                foreach (var desc in descs)
                {
                    var bracketified = desc.Name.ToString().Trim().BracketIfIdentifier();
                    sb.Append(dummyFunction[lastPos..desc.TextStart]);
                    sb.Append(bracketified);
                    lastPos = desc.End;
                }
                sb.Append(dummyFunction.Substring(lastPos));
                var replacedFunction = sb.ToString();
                parameters = replacedFunction[9..^15];
            }

            return new List<DatabaseScriptContainer> { new DatabaseScriptContainer("CreateOrAlterFunction", 40, $".create-or-alter function with({propertiesString}) {name} ({parameters}) {{ {Body} }}") };
        }
    }

}
