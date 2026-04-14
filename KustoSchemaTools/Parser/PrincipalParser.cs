using KustoSchemaTools.Model;

namespace KustoSchemaTools.Parser
{
    public static class PrincipalParser
    {
        /// <summary>
        /// Parses raw rows from .show database principals into a dictionary
        /// keyed by simplified role name (e.g., "Admin", "User").
        /// Filters out cluster-wide roles (those starting with "All").
        /// </summary>
        public static Dictionary<string, List<AADObject>> ParsePrincipals(List<PrincipalRawRow> rows)
        {
            if (rows == null)
                return new Dictionary<string, List<AADObject>>();

            return rows
                .Select(r => new
                {
                    Role = ExtractRoleName(r.Role),
                    Principal = new AADObject
                    {
                        Name = CleanDisplayName(r.PrincipalDisplayName),
                        Id = r.PrincipalFQN
                    }
                })
                .Where(r => !r.Role.StartsWith("All"))
                .GroupBy(r => r.Role)
                .ToDictionary(
                    g => g.Key,
                    g => g.Select(r => r.Principal).ToList()
                );
        }

        /// <summary>
        /// Extracts the last word from a role string.
        /// E.g., "Database Admin" → "Admin", "Database User" → "User".
        /// </summary>
        internal static string ExtractRoleName(string role)
        {
            if (string.IsNullOrWhiteSpace(role))
                return "";

            var parts = role.Split(' ');
            return parts[^1];
        }

        /// <summary>
        /// Cleans a display name by removing the parenthesized suffix and trimming.
        /// E.g., "My App (some-guid)" → "My App".
        /// </summary>
        internal static string CleanDisplayName(string displayName)
        {
            if (string.IsNullOrWhiteSpace(displayName))
                return "";

            var parenIndex = displayName.IndexOf('(');
            var name = parenIndex >= 0 ? displayName[..parenIndex] : displayName;
            return name.Trim();
        }
    }

    public class PrincipalRawRow
    {
        public string Role { get; set; }
        public string PrincipalDisplayName { get; set; }
        public string PrincipalFQN { get; set; }
    }
}
