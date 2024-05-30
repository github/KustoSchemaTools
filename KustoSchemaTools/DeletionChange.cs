using KustoSchemaTools.Changes;
using KustoSchemaTools.Parser;
using System.Text;

namespace KustoSchemaTools
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

        public List<DatabaseScriptContainer> Scripts => new List<DatabaseScriptContainer> { new DatabaseScriptContainer("Deletion",0,$".drop {EntityType} {Entity}") };
       
        public string Markdown
        {
            get
            {
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
        }


    }
}
