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
        public bool Preformatted { get; set; } = false;

        public string Body { get; set; }

        public List<DatabaseScriptContainer> CreateScripts(string name, bool isNew)
        {
            // load the non-query parts of the yaml model
            var properties = GetType().GetProperties()
                .Where(p => p.GetValue(this) != null && p.Name != "Body" && p.Name != "Parameters")
                .Select(p => $"{p.Name}=```{p.GetValue(this)}```");
            var propertiesString = string.Join(", ", properties);

            // Process function parameters to ensure proper syntax when creating Kusto function
            var parameters = Parameters;
            if (!string.IsNullOrWhiteSpace(Parameters))
            {
                // PARAMETER PROCESSING WORKFLOW:
                // 1. Create a dummy Kusto function that uses our parameters to leverage Kusto parser
                // 2. Parse the function to extract parameter declarations AST
                // 3. For each parameter name, apply bracketing if needed (for identifiers with special chars)
                // 4. Reconstruct the parameter string with properly formatted parameter names
                
                // Create a simple dummy function to parse, embedding our parameters
                var dummyFunction = $"let x = ({parameters}) {{print \"abc\"}}";
                var parsed = KustoCode.Parse(dummyFunction);

                // Extract all parameter name declarations from the parsed syntax tree
                var descs = parsed.Syntax
                    .GetDescendants<FunctionParameters>()
                    .First()
                    .GetDescendants<NameDeclaration>()
                    .ToList();

                // Rebuild the parameters string with proper bracketing for each parameter name
                var sb = new StringBuilder();
                int lastPos = 0;
                foreach (var desc in descs)
                {
                    // Apply bracketing to parameter name if needed (for identifiers with spaces or special chars)
                    var bracketified = desc.Name.ToString().Trim().BracketIfIdentifier();
                    
                    // Append everything from the last position up to the current parameter name
                    sb.Append(dummyFunction[lastPos..desc.TextStart]);
                    
                    // Append the properly bracketed parameter name
                    sb.Append(bracketified);
                    
                    // Update position tracker to end of this parameter name
                    lastPos = desc.End;
                }
                
                // Append any remaining text after the last parameter
                sb.Append(dummyFunction.Substring(lastPos));
                var replacedFunction = sb.ToString();
                
                // Extract just the parameter portion from the reconstructed dummy function
                // The slice removes "let x = (" from the start and "){print "abc"}" from the end
                parameters = replacedFunction[9..^15];
            }

            return new List<DatabaseScriptContainer> { new DatabaseScriptContainer("CreateOrAlterFunction", 40, $".create-or-alter function with({propertiesString}) {name} ({parameters}) {{ {Body} }}") };
        }
    }

}
