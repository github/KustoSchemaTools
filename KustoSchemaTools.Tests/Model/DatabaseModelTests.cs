using FluentAssertions;
using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;

namespace KustoSchemaTools.Tests.Model
{
    public class DatabaseModelTests
    {
        [Fact]
        public void Database_Should_Initialize_With_Default_Values()
        {
            // Act
            var database = new Database();

            // Assert
            database.Name.Should().BeNull();
            database.Team.Should().Be("");
            database.Monitors.Should().NotBeNull().And.BeEmpty();
            database.Viewers.Should().NotBeNull().And.BeEmpty();
            database.UnrestrictedViewers.Should().NotBeNull().And.BeEmpty();
            database.Users.Should().NotBeNull().And.BeEmpty();
            database.Ingestors.Should().NotBeNull().And.BeEmpty();
            database.Admins.Should().NotBeNull().And.BeEmpty();
            database.Tables.Should().NotBeNull().And.BeEmpty();
            database.MaterializedViews.Should().NotBeNull().And.BeEmpty();
            database.Functions.Should().NotBeNull().And.BeEmpty();
            database.ContinuousExports.Should().NotBeNull().And.BeEmpty();
            database.Scripts.Should().NotBeNull().And.BeEmpty();
            database.EntityGroups.Should().NotBeNull().And.BeEmpty();
            database.ExternalTables.Should().NotBeNull().And.BeEmpty();
            database.Metadata.Should().NotBeNull().And.BeEmpty();
            database.Deletions.Should().NotBeNull();
            database.Followers.Should().NotBeNull().And.BeEmpty();
        }

        [Fact]
        public void Database_Should_Allow_Property_Assignment()
        {
            // Arrange
            var database = new Database();
            var admin = new AADObject { Name = "admin@example.com", Id = "admin-id" };
            var table = new Table();

            // Act
            database.Name = "TestDatabase";
            database.Team = "TestTeam";
            database.Admins.Add(admin);
            database.Tables.Add("TestTable", table);

            // Assert
            database.Name.Should().Be("TestDatabase");
            database.Team.Should().Be("TestTeam");
            database.Admins.Should().ContainSingle().Which.Should().Be(admin);
            database.Tables.Should().ContainKey("TestTable");
            database.Tables["TestTable"].Should().Be(table);
        }
    }
}
