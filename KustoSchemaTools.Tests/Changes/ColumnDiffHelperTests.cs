using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Tests.Changes
{
    public class ColumnDiffHelperTests
    {
        [Fact]
        public void BuildColumnDiff_WithAddedColumns_ShowsAdditions()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" },
                { "new_col", "long" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.NotNull(result);
            Assert.Contains("+ new_col: long", result);
            Assert.DoesNotContain("id", result);
            Assert.DoesNotContain("timestamp", result);
        }

        [Fact]
        public void BuildColumnDiff_WithTypeChange_ShowsTypeChange()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "count", "int" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "count", "long" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.NotNull(result);
            Assert.Contains("! count: int → long", result);
            Assert.DoesNotContain("id", result);
        }

        [Fact]
        public void BuildColumnDiff_WithRemovedColumns_ShowsInformationalNote()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "old_col", "string" },
                { "timestamp", "datetime" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.NotNull(result);
            Assert.Contains("old_col: string  (live only)", result);
            Assert.Contains(".create-merge", result);
            Assert.DoesNotContain("+ ", result);
        }

        [Fact]
        public void BuildColumnDiff_WithNoChanges_ReturnsNull()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.Null(result);
        }

        [Fact]
        public void BuildColumnDiff_WithNullOldColumns_TreatsAsNewTable()
        {
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(null, newColumns);

            Assert.NotNull(result);
            Assert.Contains("+ id: string", result);
            Assert.Contains("+ timestamp: datetime", result);
        }

        [Fact]
        public void BuildColumnDiff_WithNullNewColumns_ReturnsNull()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, null);

            Assert.Null(result);
        }

        [Fact]
        public void BuildColumnDiff_WithMixedChanges_ShowsAllCategories()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "count", "int" },
                { "removed_col", "string" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "count", "long" },
                { "new_col", "datetime" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.NotNull(result);
            Assert.Contains("+ new_col: datetime", result);
            Assert.Contains("! count: int → long", result);
            Assert.Contains("removed_col: string  (live only)", result);
        }

        [Fact]
        public void BuildColumnDiff_CaseInsensitiveTypeComparison_IgnoresCaseDifference()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "String" }
            };
            var newColumns = new Dictionary<string, string>
            {
                { "id", "string" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.Null(result);
        }

        [Fact]
        public void BuildColumnDiff_ReorderOnly_ReturnsNull()
        {
            var oldColumns = new Dictionary<string, string>
            {
                { "id", "string" },
                { "timestamp", "datetime" },
                { "count", "long" }
            };
            // Same columns, different insertion order in the dictionary
            var newColumns = new Dictionary<string, string>
            {
                { "count", "long" },
                { "id", "string" },
                { "timestamp", "datetime" }
            };

            var result = ColumnDiffHelper.BuildColumnDiff(oldColumns, newColumns);

            Assert.Null(result);
        }
    }
}
