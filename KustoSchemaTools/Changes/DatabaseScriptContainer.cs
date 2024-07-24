using KustoSchemaTools.Model;

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

        public DatabaseScript Script { get; set; }
        public string Kind{ get; set; }
        public bool? IsValid { get; set; }
        public string Text => Script.Text;
        public int Order => Script.Order;
        public bool IsAsync { get;set; }
    }
}
