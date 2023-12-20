using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Text;

namespace KustoSchemaTools.Changes
{


    public class EntityGroupChange : BaseChange<List<Entity>>
    {
        public EntityGroupChange(string db, string entity, List<Entity>? from, List<Entity> to) : base("EntityGroup", entity, from ?? new List<Entity>(), to)
        {
            Db = db;
            Init();
        }

        public string Db { get; }

        private void Init()
        {
            var toEntityStrings = To.Select(t => $"cluster('{t.Cluster}').database('{t.Database}')").ToList();
            var fromEntityStrings = From?.Select(f => $"cluster('{f.Cluster}').database('{f.Database}')").ToList() ?? new List<string>();

            var added = toEntityStrings.Except(fromEntityStrings);
            var removed = fromEntityStrings.Except(toEntityStrings);

            var toEntityArr = string.Join(",", toEntityStrings);
            var toScript = new DatabaseScriptContainer("EntityGroup", 3, $".create-or-alter entity_group {Entity} ({toEntityArr})");

            if (added.Any() == false && removed.Any() == false)
            {
                return;
            }

            Scripts.Add(toScript);
            var code = KustoCode.Parse(toScript.Text);
            var diagnostics = code.GetDiagnostics();
            toScript.IsValid = diagnostics.Any() == false;
            var logo = toScript.IsValid.Value ? ":green_circle:" : ":red_circle:";


            var sb = new StringBuilder();
            sb.AppendLine($"## {Entity}");
            sb.AppendLine();
            sb.AppendLine("<table>");
            sb.AppendLine($"<tr></tr>");

            if (added.Any())
            {
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan=\"2\">Added:</td>");
                var addedIds = string.Join("<br>", added);
                sb.AppendLine($"<td colspan=\"10\">{addedIds}:</td>");
                sb.AppendLine("</tr>");
            }
            if (removed.Any())
            {
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan=\"2\">Removed:</td>");
                var removedIds = string.Join("<br>", removed);
                sb.AppendLine($"<td colspan=\"10\">{removedIds}:</td>");
                sb.AppendLine("</tr>");
            }
            sb.AppendLine("<tr>");
            sb.AppendLine($"<td colspan=\"2\">{logo}</td>");
            sb.AppendLine($"<td colspan=\"10\"><pre lang=\"kql\">{toScript.Script.Text.PrettifyKql()}</pre></td>");
            sb.AppendLine("</tr>");
            sb.AppendLine("</table>");

            Markdown = sb.ToString();
        }
    }

}
