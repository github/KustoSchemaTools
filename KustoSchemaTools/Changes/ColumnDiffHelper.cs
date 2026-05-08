using KustoSchemaTools.Model;
using System.Text;

namespace KustoSchemaTools.Changes
{
    /// <summary>
    /// Generates a human-readable column-level diff for table schema changes.
    /// Instead of showing full .create-merge table lines, shows only the columns
    /// that were added or had their type changed.
    /// </summary>
    public static class ColumnDiffHelper
    {
        /// <summary>
        /// Builds a column-level diff between two sets of columns.
        /// </summary>
        /// <param name="oldColumns">Columns from the live cluster state (may be null for new tables).</param>
        /// <param name="newColumns">Columns from the desired YAML state.</param>
        /// <returns>
        /// A formatted diff string showing added columns and type changes,
        /// or null if there are no meaningful column differences.
        /// </returns>
        public static string? BuildColumnDiff(Dictionary<string, string>? oldColumns, Dictionary<string, string>? newColumns)
        {
            if (newColumns == null) return null;
            oldColumns ??= new Dictionary<string, string>();

            var added = newColumns
                .Where(c => !oldColumns.ContainsKey(c.Key))
                .ToList();

            var typeChanged = newColumns
                .Where(c => oldColumns.ContainsKey(c.Key) && !string.Equals(oldColumns[c.Key], c.Value, StringComparison.OrdinalIgnoreCase))
                .Select(c => new { Name = c.Key, OldType = oldColumns[c.Key], NewType = c.Value })
                .ToList();

            var removedFromYaml = oldColumns
                .Where(c => !newColumns.ContainsKey(c.Key))
                .ToList();

            if (!added.Any() && !typeChanged.Any() && !removedFromYaml.Any())
                return null;

            var sb = new StringBuilder();
            sb.AppendLine("```diff");

            foreach (var col in added)
            {
                sb.AppendLine($"+ {col.Key}: {col.Value}");
            }

            foreach (var col in typeChanged)
            {
                sb.AppendLine($"! {col.Name}: {col.OldType} → {col.NewType}");
            }

            if (removedFromYaml.Any())
            {
                sb.AppendLine("```");
                sb.AppendLine();
                sb.AppendLine("> **Note**: The following columns exist in the live cluster but not in YAML.");
                sb.AppendLine("> `.create-merge` does not remove columns — they will remain on the table.");
                sb.AppendLine();
                sb.AppendLine("```");
                foreach (var col in removedFromYaml)
                {
                    sb.AppendLine($"  {col.Key}: {col.Value}  (live only)");
                }
            }

            sb.AppendLine("```");
            return sb.ToString();
        }
    }
}
