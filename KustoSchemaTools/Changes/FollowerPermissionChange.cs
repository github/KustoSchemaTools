using Kusto.Language;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using System.Text;

namespace KustoSchemaTools.Changes
{
    // Generates follower-safe permission commands (add/drop) for admins/viewers.
    public class FollowerPermissionChange : BaseChange<List<AADObject>>
    {
        public FollowerPermissionChange(string db, string entity, List<AADObject> from, List<AADObject> to, string? leaderName)
            : base("FollowerPermissions", entity, from ?? new List<AADObject>(), to)
        {
            Db = db;
            LeaderName = leaderName;
            Init();
        }

        public string Db { get; }
        public string? LeaderName { get; }

        private static IEnumerable<string> PrincipalStrings(IEnumerable<AADObject> principals) =>
            principals.OrderBy(p => p.Id, StringComparer.OrdinalIgnoreCase).Select(a => "\"" + a.Id + "\"");

        private string BuildAdd(IEnumerable<AADObject> principals)
        {
            var ids = string.Join(",", PrincipalStrings(principals));
            var leaderSuffix = string.IsNullOrWhiteSpace(LeaderName) ? string.Empty : $" '{LeaderName}'";
            return $".add follower database {Db.BracketIfIdentifier()} {Entity.ToLower()} ({ids}){leaderSuffix}";
        }

        private string BuildDrop(IEnumerable<AADObject> principals)
        {
            var ids = string.Join(",", PrincipalStrings(principals));
            return $".drop follower database {Db.BracketIfIdentifier()} {Entity.ToLower()} ({ids})";
        }

        private void Init()
        {
            var added = To.Where(itm => From.All(t => t.Id != itm.Id)).ToList();
            var removed = From.Where(itm => To.All(t => t.Id != itm.Id)).ToList();
            var changed = From.Join(To, f => f.Id, t => t.Id, (f, t) => new { f, t })
                              .Where(x => x.f.Name != x.t.Name)
                              .ToList();

            if (removed.Any())
            {
                // Execute drops before adds; keep non-negative so they aren't filtered out.
                var script = new DatabaseScript { Text = BuildDrop(removed), Order = 0 };
                var container = new DatabaseScriptContainer(script, "FollowerPermissionChange");
                container.IsValid = !KustoCode.Parse(script.Text).GetDiagnostics().Any();
                Scripts.Add(container);
            }

            if (added.Any())
            {
                var script = new DatabaseScript { Text = BuildAdd(added), Order = removed.Any() ? 1 : 0 };
                var container = new DatabaseScriptContainer(script, "FollowerPermissionChange");
                container.IsValid = !KustoCode.Parse(script.Text).GetDiagnostics().Any();
                Scripts.Add(container);
            }

            if (changed.Any())
            {
                Scripts.Add(new DatabaseScriptContainer("FollowerPermissionRenamed", -1, "// No Database Change"));
            }

            var sb = new StringBuilder();
            sb.AppendLine($"## {Entity} (Follower)");
            sb.AppendLine();
            sb.AppendLine("<table>");
            sb.AppendLine("<tr></tr>");

            if (added.Any())
            {
                sb.AppendLine("<tr><td colspan=\"2\">Added:</td><td colspan=\"10\">" + string.Join("<br>", added.Select(t => $"{t.Name} ({t.Id})")) + "</td></tr>");
            }
            if (removed.Any())
            {
                sb.AppendLine("<tr><td colspan=\"2\">Removed:</td><td colspan=\"10\">" + string.Join("<br>", removed.Select(t => $"{t.Name} ({t.Id})")) + "</td></tr>");
            }
            if (changed.Any())
            {
                sb.AppendLine("<tr><td colspan=\"2\">Changed:</td><td colspan=\"10\">" + string.Join("<br>", changed.Select(t => $"{t.f.Name} => {t.t.Name} ({t.t.Id})")) + "</td></tr>");
            }

            var logo = Scripts.Any() && Scripts.First().IsValid == false ? ":red_circle:" : ":green_circle:";
            var displayCmd = Scripts.FirstOrDefault()?.Script?.Text ?? "// No change";
            sb.AppendLine($"<tr><td colspan=\"2\">{logo}</td><td colspan=\"10\"><pre lang=\"kql\">{displayCmd.PrettifyKql()}</pre></td></tr>");
            sb.AppendLine("</table>");

            Markdown = sb.ToString();
        }
    }
}
