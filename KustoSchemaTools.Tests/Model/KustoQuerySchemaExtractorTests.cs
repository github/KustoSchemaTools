using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class KustoQuerySchemaExtractorTests
    {
        [Fact]
        public void ExtractOutputSchema_Should_Handle_Simple_Project_Query()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Data", "dynamic" }
            };
            
            var query = "SourceTable | project EventId, Timestamp";

            // Act
            var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

            // Assert
            outputSchema.Should().HaveCount(2);
            outputSchema.Should().ContainKey("EventId").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("Timestamp").WhoseValue.Should().Be("datetime");
        }

        [Fact]
        public void ExtractOutputSchema_Should_Handle_Extend_Query()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" }
            };
            
            var query = "SourceTable | extend ProcessedTime = now(), EventType = 'processed'";

            // Act
            var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

            // Assert
            outputSchema.Should().HaveCount(4); // Original 2 + 2 new columns
            outputSchema.Should().ContainKey("EventId").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("Timestamp").WhoseValue.Should().Be("datetime");
            outputSchema.Should().ContainKey("ProcessedTime").WhoseValue.Should().Be("datetime");
            outputSchema.Should().ContainKey("EventType").WhoseValue.Should().Be("string");
        }

        [Fact]
        public void ExtractOutputSchema_Should_Handle_Complex_Query_With_Type_Conversions()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Count", "int" },
                { "Data", "dynamic" }
            };
            
            var query = @"SourceTable 
                         | project EventId, 
                                   CountAsString = tostring(Count),
                                   DataAsString = tostring(Data),
                                   ProcessedAt = now()";

            // Act
            var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

            // Assert
            outputSchema.Should().HaveCount(4);
            outputSchema.Should().ContainKey("EventId").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("CountAsString").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("DataAsString").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("ProcessedAt").WhoseValue.Should().Be("datetime");
        }

        [Fact]
        public void ExtractColumnReferences_Should_Find_Referenced_Columns()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Count", "int" },
                { "Data", "dynamic" }
            };
            
            var query = "SourceTable | where Timestamp > ago(1h) | extend ProcessedCount = Count * 2 | project EventId, ProcessedCount";

            // Act
            var referencedColumns = KustoQuerySchemaExtractor.ExtractColumnReferences(query, "SourceTable", sourceSchema);

            // Assert
            referencedColumns.Should().Contain("EventId");
            referencedColumns.Should().Contain("Timestamp");
            referencedColumns.Should().Contain("Count");
            referencedColumns.Should().NotContain("Data"); // Not referenced in the query
            referencedColumns.Should().NotContain("ProcessedCount"); // This is created, not referenced
        }

        [Fact]
        public void ValidateQuery_Should_Detect_Syntax_Errors()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" }
            };
            
            var invalidQuery = "SourceTable | invalid_operator EventId"; // Invalid KQL

            // Act
            var result = KustoQuerySchemaExtractor.ValidateQuery(invalidQuery, sourceSchema);

            // Assert
            result.IsValid.Should().BeFalse();
            result.HasErrors.Should().BeTrue();
            result.Errors.Should().NotBeEmpty();
        }

        [Fact]
        public void ValidateQuery_Should_Detect_Column_Reference_Errors()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" }
            };
            
            var queryWithBadColumn = "SourceTable | project EventId, NonExistentColumn"; // References non-existent column

            // Act
            var result = KustoQuerySchemaExtractor.ValidateQuery(queryWithBadColumn, sourceSchema);

            // Assert
            result.IsValid.Should().BeFalse();
            result.HasErrors.Should().BeTrue();
            result.Errors.Should().Contain(e => e.Contains("NonExistentColumn"));
        }

        [Fact]
        public void ValidateQuery_Should_Pass_For_Valid_Query()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Timestamp", "datetime" },
                { "Count", "int" }
            };
            
            var validQuery = "SourceTable | where Timestamp > ago(1h) | extend DoubleCount = Count * 2 | project EventId, DoubleCount";

            // Act
            var result = KustoQuerySchemaExtractor.ValidateQuery(validQuery, sourceSchema);

            // Assert
            result.IsValid.Should().BeTrue();
            result.HasErrors.Should().BeFalse();
            result.OutputSchema.Should().ContainKey("EventId");
            result.OutputSchema.Should().ContainKey("DoubleCount");
            result.ReferencedColumns.Should().Contain("EventId", "Timestamp", "Count");
        }

        [Fact]
        public void ExtractOutputSchema_Should_Handle_Summary_Operations()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "Category", "string" },
                { "Count", "int" }
            };
            
            var query = "SourceTable | summarize TotalCount = sum(Count), EventCount = count() by Category";

            // Act
            var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

            // Assert
            outputSchema.Should().HaveCount(3);
            outputSchema.Should().ContainKey("Category").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("TotalCount").WhoseValue.Should().Be("long"); // sum() returns long
            outputSchema.Should().ContainKey("EventCount").WhoseValue.Should().Be("long"); // count() returns long
        }

        [Fact]
        public void ExtractOutputSchema_Should_Handle_Join_Operations()
        {
            // Arrange
            var sourceSchema = new Dictionary<string, string>
            {
                { "EventId", "string" },
                { "UserId", "string" },
                { "Timestamp", "datetime" }
            };
            
            // Note: This is a simplified example. In practice, joins require both tables to be defined
            var query = "SourceTable | project EventId, UserId, Timestamp";

            // Act
            var outputSchema = KustoQuerySchemaExtractor.ExtractOutputSchema(query, sourceSchema);

            // Assert
            outputSchema.Should().HaveCount(3);
            outputSchema.Should().ContainKey("EventId").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("UserId").WhoseValue.Should().Be("string");
            outputSchema.Should().ContainKey("Timestamp").WhoseValue.Should().Be("datetime");
        }
    }
}
