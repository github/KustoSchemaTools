using KustoSchemaTools.Model;

namespace KustoSchemaTools.Changes
{
    public class DatabaseScriptContainer
    {
        public DatabaseScriptContainer()
        {
            
        }

        public DatabaseScriptContainer(DatabaseScript script, string kind, bool? isValid = null)
        {
            Script = script;
            Kind = kind;
            IsValid = isValid;
        }

        public DatabaseScriptContainer(string kind, int order, string script, bool? isValid = null)
        {
            Script = new DatabaseScript(script, order);
            Kind = kind;
            IsValid = isValid;
        }

        public DatabaseScript Script { get; set; }
        public string Kind{ get; set; }
        public bool? IsValid { get; set; }
        public string Text => Script.Text;
        public int Order => Script.Order;
    }
}
