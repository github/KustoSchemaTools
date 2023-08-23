using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Text;

namespace KustoSchemaTools.Changes
{


    public class PermissionChange : BaseChange<List<AADObject>>
    {
        public PermissionChange(string db, string entity, List<AADObject>? from, List<AADObject> to) : base("Permissions", entity, from ?? new List<AADObject>(), to)
        {
            Db = db;
            Init();
        }

        public string Db { get; }

        private void Init()
        {
            DatabaseScript toScript;
            DatabaseScript fromScript;
            if (To.Count > 0)
            {
                var x = string.Join(",", To.Select(a => "\"" + a.Id + "\""));
                toScript = new DatabaseScript { Text = $".set database {Db} {Entity.ToLower()} ({x})", Order = 0 };
            }
            else
            {
                toScript = new DatabaseScript { Text = $".set database {Db} {Entity.ToLower()} none", Order = 0 };
            }
            if (To.Count > 0)
            {
                var x = string.Join(",", From.Select(a => "\"" + a.Id + "\""));
                fromScript = new DatabaseScript { Text = $".set database {Db} {Entity.ToLower()} ({x})", Order = 0 };
            }
            else
            {
                fromScript = new DatabaseScript { Text = $".set database {Db} {Entity.ToLower()} none", Order = 0 };
            }


            var script = "No database changes";

            var scriptContainer = new DatabaseScriptContainer(toScript, "");

            if (fromScript.Text != toScript.Text)
            {
                Scripts.Add(scriptContainer);
                script = toScript.Text;
            }

            var code = KustoCode.Parse(toScript.Text);
            var diagnostics = code.GetDiagnostics();
            scriptContainer.IsValid = diagnostics.Any() == false;
            var logo = scriptContainer.IsValid.Value ? ":green_circle:" : ":red_circle:";

            var changed = From
                .Join(To, itm => itm.Id, itm => itm.Id, (from, to) => new { from, to })
                .Where(itm => itm.from.Name != itm.to.Name)
                .ToList();
            var added = To.Where(itm => From.All(t => t.Id != itm.Id)).ToList();
            var removed = From.Where(itm => To.All(t => t.Id != itm.Id)).ToList();

            var sb = new StringBuilder();
            sb.AppendLine($"## {Entity}");
            sb.AppendLine();
            sb.AppendLine("<table>");
            sb.AppendLine($"<tr></tr>");



            if (added.Any())
            {
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan=\"2\">Added:</td>");
                var addedIds = string.Join("<br>", added.Select(t => $"{t.Name} ({t.Id})"));
                sb.AppendLine($"<td colspan=\"10\">{addedIds}:</td>");
                sb.AppendLine("</tr>");
            }
            if (removed.Any())
            {
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan=\"2\">Removed:</td>");
                var removedIds = string.Join("<br>", removed.Select(t => $"{t.Name} ({t.Id})"));
                sb.AppendLine($"<td colspan=\"10\">{removedIds}:</td>");
                sb.AppendLine("</tr>");
            }
            if (changed.Any())
            {
                Scripts.Add(new DatabaseScriptContainer("PermissionRenamed", -1,"// No Database Change"));
                sb.AppendLine("<tr>");
                sb.AppendLine("<td colspan=\"2\">Changed:</td>");
                var changedIds = string.Join("<br>", changed.Select(t => $"{t.from.Name} => {t.to.Name} ({t.to.Id})"));
                sb.AppendLine($"<td colspan=\"10\">{changedIds}:</td>");
                sb.AppendLine("</tr>");
            }
            sb.AppendLine("<tr>");
            sb.AppendLine($"<td colspan=\"2\">{logo}</td>");
            sb.AppendLine($"<td colspan=\"10\"><pre lang=\"kql\">{script.PrettifyKql()}</pre></td>");
            sb.AppendLine("</tr>");
            sb.AppendLine("</table>");


            Markdown = sb.ToString();
        }
    }

}
