using KustoSchemaTools.Helpers;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Plugins;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Parser
{
    public class YamlDatabaseParserTests
    {
        const string BasePath = "DemoData";
        const string Deployment = "DemoDeployment";
        const string Database = "DemoDatabase";

        [Fact]
        public async Task GetDatabase()
        {
            var factory = new YamlDatabaseHandlerFactory<Model.Database>()
                .WithPlugin(new TablePlugin())
                .WithPlugin(new FunctionPlugin())
                .WithPlugin(new MaterializedViewsPlugin())
                .WithPlugin(new DatabaseCleanup());
            var loader = factory.Create(Path.Combine(BasePath, Deployment), Database);

            var db = await loader.LoadAsync();

            Assert.NotNull(db);
            Assert.Equal(2, db.Tables.Count);
            Assert.Equal(1, db.Functions.Count);
            Assert.Equal(6, db.Functions["UP"].Body.RowLength());
            Assert.Equal("DemoDatabase", db.Name);
            Assert.True(db.Tables["sourceTable"].RestrictedViewAccess);

            // these tests do not compile! to be removed in a future PR.
            // Assert.Equal("120d", db.Tables["tableWithUp"].RetentionAndCachePolicy.Retention);
            // Assert.Equal("120d", db.Tables["sourceTable"].RetentionAndCachePolicy.HotCache);
        }
    }
}
