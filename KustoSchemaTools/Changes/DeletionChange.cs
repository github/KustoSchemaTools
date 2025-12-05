using KustoSchemaTools.Parser;
using System.Text;
using Kusto.Language;


namespace KustoSchemaTools.Changes
{
    public class DeletionChange : IChange
    {
        public DeletionChange(string entity, string entityType)
        {
            EntityType = entityType;
            Entity = entity;
        }

        public string EntityType { get; set; }

        public string Entity { get; set; }

        public List<DatabaseScriptContainer> Scripts
        {
            get
            {
                var sc = new DatabaseScriptContainer("Deletion", 0, $".drop {EntityType} {Entity}");
                var code = KustoCode.Parse(sc.Script.Text);
                var diagnostics = code.GetDiagnostics();
                sc.IsValid = diagnostics.Any() == false;
                return new List<DatabaseScriptContainer> { sc };
            }
        }

        private string? _customMarkdown;

        public string Markdown
        {
            get
            {
                if (!string.IsNullOrEmpty(_customMarkdown))
                {
                    return _customMarkdown;
                }

                var sb = new StringBuilder();
                sb.AppendLine($"## {Entity}");
                sb.AppendLine();
                sb.AppendLine("<table>");
                sb.AppendLine($"<tr></tr>");

                sb.AppendLine("<tr>");
                sb.AppendLine($"<td colspan=\"2\">:recycle:</td>");
                sb.AppendLine($"<td colspan=\"10\"><pre lang=\"kql\">{Scripts[0].Script.Text.PrettifyKql()}</pre></td>");
                sb.AppendLine("</tr>");
                sb.AppendLine("</table>");

                return sb.ToString();
            }
            set
            {
                _customMarkdown = value;
            }
        }


        public Comment Comment { get; set; }
    }
}
