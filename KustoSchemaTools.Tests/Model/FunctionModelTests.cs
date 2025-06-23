using FluentAssertions;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Model
{
    public class FunctionModelTests
    {
        [Fact]
        public void Function_Should_Initialize_With_Default_Values()
        {
            // Act
            var function = new Function();

            // Assert
            function.Body.Should().BeNull();
            function.Folder.Should().Be("");
            function.DocString.Should().Be("");
            function.Parameters.Should().Be("");
            function.SkipValidation.Should().BeFalse();
            function.View.Should().BeFalse();
            function.Preformatted.Should().BeFalse();
        }

        [Fact]
        public void Function_Should_Allow_Property_Assignment()
        {
            // Arrange
            var function = new Function();

            // Act
            function.Body = "T | count";
            function.Folder = "Analytics";
            function.DocString = "Counts rows in table T";
            function.Parameters = "(T: (*)";
            function.SkipValidation = true;
            function.View = true;

            // Assert
            function.Body.Should().Be("T | count");
            function.Folder.Should().Be("Analytics");
            function.DocString.Should().Be("Counts rows in table T");
            function.Parameters.Should().Be("(T: (*)");
            function.SkipValidation.Should().BeTrue();
            function.View.Should().BeTrue();
        }

        [Fact]
        public void Function_Should_Generate_Creation_Script()
        {
            // Arrange
            var function = new Function
            {
                Body = "StormEvents | count\n",
                Folder = "Weather",
                DocString = "Count storm events",
                Parameters = "tableName: string" // Provide valid parameters
            };

            // Act
            var scripts = function.CreateScripts("CountStormEvents", true);

            // Assert
            scripts.Should().NotBeEmpty();
            scripts.Should().HaveCount(1);
            var script = scripts.First();
            script.Kind.Should().Be("CreateOrAlterFunction");
            script.Script.Text.Should().Contain(".create-or-alter function");
            script.Script.Text.Should().Contain("CountStormEvents");
            script.Script.Text.Should().Contain("StormEvents | count");
            script.Script.Text.Should().Contain("Folder=```Weather```");
            script.Script.Text.Should().Contain("DocString=```Count storm events```");
        }
    }
}
