using DiffPlex.DiffBuilder;
using DiffPlex;
using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Text;
using System.Data;
using DiffPlex.DiffBuilder.Model;
using Kusto.Language.Editor;

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
            var from = From?.CreateScripts(Entity, false).ToDictionary(itm => itm.Kind) ?? new Dictionary<string, DatabaseScriptContainer>();
            var to = To.CreateScripts(Entity, from == null);
            Markdown = string.Empty;

            if (to.Any() == false) return;

            StringBuilder sb = new StringBuilder($"## {Entity}");
            sb.AppendLine();
            sb.AppendLine("<table>");

            foreach (var change in to)
            {
                var before = from.ContainsKey(change.Kind) ? from[change.Kind] : null;
                var beforeText = before?.Text ?? "";
                var afterText = change.Text;

                var singleLinebeforeText = new KustoCodeService(KustoCode.Parse(beforeText)).GetMinimalText(MinimalTextKind.SingleLine);
                var singleLineafterText = new KustoCodeService(KustoCode.Parse(afterText)).GetMinimalText(MinimalTextKind.SingleLine);

                var reducedBefore = KustoCode.Parse(singleLinebeforeText);
                var reducedAfter = KustoCode.Parse(singleLineafterText);

                var zipped = reducedBefore.GetLexicalTokens().Zip(reducedAfter.GetLexicalTokens()).ToList();
                var diffs = zipped.Where(itm => itm.First.Text != itm.Second.Text).ToList();
                if(diffs.Any() == false) continue;

                if (singleLinebeforeText.Equals(singleLineafterText)) continue;

                var differ = new Differ();
                var diff = InlineDiffBuilder.Diff(beforeText, afterText, true);
                if (diff.Lines.All(itm => itm.Type == ChangeType.Unchanged)) continue;

                var code = KustoCode.Parse(change.Text);                

                var diagnostics = code.GetDiagnostics();
                change.IsValid = diagnostics.Any() == false || change.Order == -1;
                Scripts.Add(change);     


                var logo = change.IsValid.Value ? ":green_circle:" : ":red_circle:";
                var addActionText = before == null ? "Add" : "To";
                sb.AppendLine($"<tr></tr>");
                sb.AppendLine("<tr>");
                sb.AppendLine($"<td colspan=\"1\"><b>{logo}<b></td>");
                sb.AppendLine($"<td colspan =\"11\"><b>{change.Kind}<b></td>");
                sb.AppendLine($"</tr>");
                if (before != null)
                {
                    foreach(var c in new [] { new { Change = ChangeType.Deleted, Prefix = "-" }, new { Change = ChangeType.Inserted, Prefix = "+" } })
                    {
                        var changeType = c.Change;
                        if (diff.Lines.Any(itm => itm.Type == changeType))
                        {
                            sb.AppendLine("<tr>");
                            sb.AppendLine($"    <td colspan=\"2\">{c.Change}:</td>");
                            sb.AppendLine($"    <td colspan=\"10\"> \n\n```diff ");
                            
                            var relevantLines = diff.Lines.Where(itm => itm.Type == ChangeType.Unchanged || itm.Type == changeType).OrderBy(itm => itm.Position).ToList();
                            int last = 0;
                            for (int i = 0; i < relevantLines.Count; i++)
                            {
                                var b = i - 1 > 0 ? relevantLines[i - 1] : null;
                                var current = relevantLines[i];
                                var n = i + 1 < relevantLines.Count ? relevantLines[i + 1] : null;

                                if (current.Type == changeType || b?.Type == changeType  || n?.Type == changeType)
                                {
                                    if(i-last > 1)
                                    {
                                        sb.AppendLine();
                                    }

                                    var p = current.Type == changeType ? c.Prefix : " ";
                                    sb.AppendLine($"{p}{i}:\t{current.Text}");
                                    last = i;
                                }
                            }
                            sb.AppendLine("```\n\n</td></tr>");
                        }
                    }
                }
                sb.AppendLine("<tr>");
                sb.AppendLine($"    <td colspan=\"2\">Script:</td>");
                sb.AppendLine($"    <td colspan=\"10\"><pre lang=\"kql\">{change.Text.PrettifyKql()}</pre></td>");
                sb.AppendLine("</tr>");

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
