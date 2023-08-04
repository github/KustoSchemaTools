using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Diagnostics.Metrics;
using System.Text;

namespace KustoSchemaTools.Changes
{


    public class ScriptCompareChange : BaseChange<IKustoBaseEntity>
    {
        public ScriptCompareChange(string entity, IKustoBaseEntity? from, IKustoBaseEntity to) : base(to.GetType().Name, entity, from, to)
        {
            Init();
        }


        private void Init()
        {
            var from = From?.CreateScripts(Entity).ToDictionary(itm => itm.Kind) ?? new Dictionary<string, DatabaseScriptContainer>();
            var to = To.CreateScripts(Entity);
            Markdown = string.Empty;

            if (to.Any() == false) return;


            var changedTo = to.Where(itm => !from.ContainsKey(itm.Kind) || itm.Text.Equals(from[itm.Kind].Text) == false).ToList();

            StringBuilder sb = new StringBuilder($"## {Entity}");
            sb.AppendLine();
            sb.AppendLine("<table>");

            foreach (var change in changedTo)
            {

                var code = KustoCode.Parse(change.Text);
                var diagnostics = code.GetDiagnostics();

                change.IsValid = diagnostics.Any() == false || change.Order == -1;
                Scripts.Add(change);

                var before = from.ContainsKey(change.Kind) ? from[change.Kind] : null;
                var logo = change.IsValid.Value ? ":green_circle:" : ":red_circle:";
                var addActionText = before == null ? "Add" : "To";
                sb.AppendLine($"<tr></tr>");
                sb.AppendLine("<tr>");
                sb.AppendLine($"<td colspan=\"1\"><b>{logo}<b></td>");
                sb.AppendLine($"<td colspan =\"11\"><b>{change.Kind}<b></td>");
                sb.AppendLine($"</tr>");
                if (before != null)
                {






                    sb.AppendLine("<tr>");
                    sb.AppendLine($"    <td colspan=\"2\">From:</td>");
                    sb.AppendLine($"    <td colspan=\"10\"><pre lang=\"kql\">{before.Text.PrettifyKql()}</pre></td>");
                    sb.AppendLine("</tr>");
                }
                else
                {
                    sb.AppendLine("<tr>");
                    sb.AppendLine($"    <td colspan=\"2\">{addActionText}:</td>");
                    sb.AppendLine($"    <td colspan=\"10\"><pre lang=\"kql\">{change.Text.PrettifyKql()}</pre></td>");
                    sb.AppendLine("</tr>");
                }
                if (change.IsValid == false)
                {
                    foreach (var diagnostic in diagnostics)
                    {
                        sb.AppendLine("<tr>");
                        sb.AppendLine($"    <td colspan=\"2\">{diagnostic.Start}-{diagnostic.End}</td>");
                        sb.AppendLine($"    <td colspan=\"10\">{diagnostic.Description}</td>");
                        sb.AppendLine("</tr>");
                    }
                }
            }
            sb.AppendLine("</table>");
            Markdown = sb.ToString();
        }
    }

}
