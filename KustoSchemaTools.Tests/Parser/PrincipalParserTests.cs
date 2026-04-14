using KustoSchemaTools.Parser;

namespace KustoSchemaTools.Tests.Parser
{
    public class PrincipalParserTests
    {
        [Fact]
        public void ParsePrincipals_BasicRoles_GroupsBySimplifiedRoleName()
        {
            var rows = new List<PrincipalRawRow>
            {
                new() { Role = "Database Admin", PrincipalDisplayName = "Admin App", PrincipalFQN = "aadapp=aaa;tenant" },
                new() { Role = "Database User", PrincipalDisplayName = "User App", PrincipalFQN = "aadapp=bbb;tenant" },
            };

            var result = PrincipalParser.ParsePrincipals(rows);

            Assert.True(result.ContainsKey("Admin"));
            Assert.Single(result["Admin"]);
            Assert.Equal("Admin App", result["Admin"][0].Name);
            Assert.Equal("aadapp=aaa;tenant", result["Admin"][0].Id);

            Assert.True(result.ContainsKey("User"));
            Assert.Single(result["User"]);
        }

        [Fact]
        public void ParsePrincipals_FiltersOutAllRoles()
        {
            var rows = new List<PrincipalRawRow>
            {
                new() { Role = "Database Admin", PrincipalDisplayName = "Admin", PrincipalFQN = "aadapp=aaa;t" },
                new() { Role = "AllDatabasesAdmin", PrincipalDisplayName = "Cluster Admin", PrincipalFQN = "aadapp=bbb;t" },
                new() { Role = "AllDatabasesViewer", PrincipalDisplayName = "Cluster Viewer", PrincipalFQN = "aadgroup=ccc;t" },
            };

            var result = PrincipalParser.ParsePrincipals(rows);

            Assert.Single(result);
            Assert.True(result.ContainsKey("Admin"));
            Assert.False(result.ContainsKey("AllDatabasesAdmin"));
        }

        [Fact]
        public void ParsePrincipals_MultipleAdmins_GroupedTogether()
        {
            var rows = new List<PrincipalRawRow>
            {
                new() { Role = "Database Admin", PrincipalDisplayName = "App One", PrincipalFQN = "aadapp=111;t" },
                new() { Role = "Database Admin", PrincipalDisplayName = "App Two", PrincipalFQN = "aadapp=222;t" },
            };

            var result = PrincipalParser.ParsePrincipals(rows);

            Assert.Equal(2, result["Admin"].Count);
        }

        [Fact]
        public void ParsePrincipals_NullInput_ReturnsEmptyDictionary()
        {
            var result = PrincipalParser.ParsePrincipals(null);

            Assert.Empty(result);
        }

        [Fact]
        public void ParsePrincipals_EmptyInput_ReturnsEmptyDictionary()
        {
            var result = PrincipalParser.ParsePrincipals(new List<PrincipalRawRow>());

            Assert.Empty(result);
        }

        [Fact]
        public void CleanDisplayName_RemovesParenthesizedSuffix()
        {
            Assert.Equal("My App", PrincipalParser.CleanDisplayName("My App (some-guid)"));
            Assert.Equal("Simple Name", PrincipalParser.CleanDisplayName("Simple Name"));
            Assert.Equal("", PrincipalParser.CleanDisplayName(""));
            Assert.Equal("", PrincipalParser.CleanDisplayName(null));
        }

        [Fact]
        public void CleanDisplayName_TrimsWhitespace()
        {
            Assert.Equal("My App", PrincipalParser.CleanDisplayName("  My App  "));
            Assert.Equal("My App", PrincipalParser.CleanDisplayName("My App (guid)  "));
        }

        [Fact]
        public void ExtractRoleName_TakesLastWord()
        {
            Assert.Equal("Admin", PrincipalParser.ExtractRoleName("Database Admin"));
            Assert.Equal("User", PrincipalParser.ExtractRoleName("Database User"));
            Assert.Equal("UnrestrictedViewer", PrincipalParser.ExtractRoleName("Database UnrestrictedViewer"));
            Assert.Equal("", PrincipalParser.ExtractRoleName(""));
            Assert.Equal("", PrincipalParser.ExtractRoleName(null));
        }

        [Fact]
        public void ParsePrincipals_AllKnownRoles_MappedCorrectly()
        {
            var rows = new List<PrincipalRawRow>
            {
                new() { Role = "Database Admin", PrincipalDisplayName = "A1", PrincipalFQN = "aadapp=1;t" },
                new() { Role = "Database User", PrincipalDisplayName = "U1", PrincipalFQN = "aadapp=2;t" },
                new() { Role = "Database Viewer", PrincipalDisplayName = "V1", PrincipalFQN = "aadgroup=3;t" },
                new() { Role = "Database UnrestrictedViewer", PrincipalDisplayName = "UV1", PrincipalFQN = "aadapp=4;t" },
                new() { Role = "Database Ingestor", PrincipalDisplayName = "I1", PrincipalFQN = "aadapp=5;t" },
                new() { Role = "Database Monitor", PrincipalDisplayName = "M1", PrincipalFQN = "aadapp=6;t" },
            };

            var result = PrincipalParser.ParsePrincipals(rows);

            Assert.Equal(6, result.Count);
            Assert.True(result.ContainsKey("Admin"));
            Assert.True(result.ContainsKey("User"));
            Assert.True(result.ContainsKey("Viewer"));
            Assert.True(result.ContainsKey("UnrestrictedViewer"));
            Assert.True(result.ContainsKey("Ingestor"));
            Assert.True(result.ContainsKey("Monitor"));
        }
    }
}
