using KustoSchemaTools.Helpers;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Plugins;
using KustoSchemaTools.Model;
using KustoSchemaTools.Changes;
using Kusto.Data;
using System.IO;

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
                .WithPlugin(new DatabaseCleanup());
            var loader = factory.Create(Path.Combine(BasePath, Deployment), Database);

            var db = await loader.LoadAsync();

            Assert.NotNull(db);
            Assert.Equal(2, db.Tables.Count);
            Assert.Equal(4, db.Functions.Count);
            Assert.Equal(6, db.Functions["UP"].Body.RowLength());
            Assert.Equal("DemoDatabase", db.Name);

            var st = db.Tables["sourceTable"];
            Assert.NotNull(st);
            Assert.NotNull(st.Policies);
            Assert.True(st.Policies!.RestrictedViewAccess);
            Assert.Equal("120d", st.Policies?.HotCache);

            var tt = db.Tables["tableWithUp"];
            Assert.NotNull(tt);
            Assert.NotNull(tt.Policies);
            Assert.False(tt.Policies!.RestrictedViewAccess);
            Assert.Equal("120d", tt.Policies?.Retention);
        }

        [Fact]
        public async Task VerifyFunctionPreformatted()
        {
            // WITHOUT the DatabaseCleanup plugin
            var factoryWithoutCleanup = new YamlDatabaseHandlerFactory<Model.Database>()
                .WithPlugin(new TablePlugin())
                .WithPlugin(new FunctionPlugin());
            // DatabaseCleanup intentionally omitted
            var loaderWithoutCleanup = factoryWithoutCleanup.Create(Path.Combine(BasePath, Deployment), Database);
            var dbWithoutCleanup = await loaderWithoutCleanup.LoadAsync();

            // with the DatabaseCleanup plugin
            var factoryWithCleanup = new YamlDatabaseHandlerFactory<Model.Database>()
                .WithPlugin(new TablePlugin())
                .WithPlugin(new FunctionPlugin())
                .WithPlugin(new MaterializedViewsPlugin())
                .WithPlugin(new DatabaseCleanup());
            var loaderWithCleanup = factoryWithCleanup.Create(Path.Combine(BasePath, Deployment), Database);
            var dbWithCleanup = await loaderWithCleanup.LoadAsync();

            // Assert
            Assert.NotNull(dbWithCleanup);
            Assert.NotNull(dbWithoutCleanup);
            Assert.Equal(dbWithCleanup.Functions.Count, dbWithoutCleanup.Functions.Count);

            // Verify the UP function has preformatted set to false (default)
            var up_withCleanup = dbWithCleanup.Functions["UP"];
            var up_withoutCleanup = dbWithoutCleanup.Functions["UP"];
            Assert.NotNull(up_withCleanup);
            Assert.NotNull(up_withoutCleanup);
            Assert.False(up_withCleanup.Preformatted);
            Assert.False(up_withoutCleanup.Preformatted);

            // this case is simple and formatting has no impact.
            Assert.Equal(up_withoutCleanup.Body.RowLength(), up_withCleanup.Body.RowLength());

            // Verify the needs_formatting query changed when formatting. 
            var f_withCleanup = dbWithCleanup.Functions["needs_formatting"];
            var f_withoutCleanup = dbWithoutCleanup.Functions["needs_formatting"];
            Assert.NotNull(f_withCleanup);
            Assert.NotNull(f_withoutCleanup);
            Assert.False(f_withCleanup.Preformatted);
            Assert.False(f_withoutCleanup.Preformatted);

            // preformatted function should have been formatted by DatabaseCleanup
            Assert.NotEqual(f_withCleanup.Body, f_withoutCleanup.Body);

            // much more complicated function where formatting breaks the query
            var complicated_with_cleanup = dbWithCleanup.Functions["complicated"].Body;
            var complicated_without_cleanup = dbWithoutCleanup.Functions["complicated"].Body;
            Assert.NotEqual(complicated_with_cleanup, complicated_without_cleanup);

            var complicated_pf_with_cleanup = dbWithCleanup.Functions["complicated_preformatted"].Body;
            var complicated_pf_without_cleanup = dbWithoutCleanup.Functions["complicated_preformatted"].Body;

            // preformatted option makes query match non-formatted version
            Assert.Equal(complicated_pf_without_cleanup, complicated_pf_with_cleanup);

            // preformatted option makes query match non-formatted version
            Assert.Equal(complicated_without_cleanup, complicated_pf_with_cleanup);
        }

        [Fact]
        public async Task VerifyMaterializedView()
        {
            // WITHOUT the DatabaseCleanup plugin
            var factoryWithoutCleanup = new YamlDatabaseHandlerFactory<Model.Database>()
                .WithPlugin(new TablePlugin())
                .WithPlugin(new MaterializedViewsPlugin());
            // DatabaseCleanup intentionally omitted
            var loaderWithoutCleanup = factoryWithoutCleanup.Create(Path.Combine(BasePath, Deployment), Database);
            var dbWithoutCleanup = await loaderWithoutCleanup.LoadAsync();

            // with the DatabaseCleanup plugin
            var factoryWithCleanup = new YamlDatabaseHandlerFactory<Model.Database>()
                .WithPlugin(new TablePlugin())
                .WithPlugin(new MaterializedViewsPlugin())
                .WithPlugin(new DatabaseCleanup());
            var loaderWithCleanup = factoryWithCleanup.Create(Path.Combine(BasePath, Deployment), Database);
            var dbWithCleanup = await loaderWithCleanup.LoadAsync();

            // Assert
            Assert.NotNull(dbWithCleanup);
            Assert.NotNull(dbWithoutCleanup);
            Assert.Equal(dbWithCleanup.MaterializedViews.Count, dbWithoutCleanup.MaterializedViews.Count);

            // basic materialized view tests
            void AssertMaterializedView(
                string file_name,
                bool should_match)
            {
                var mv_with_cleanup = dbWithCleanup.MaterializedViews[file_name];
                var mv_without_cleanup = dbWithoutCleanup.MaterializedViews[file_name];
                Assert.NotNull(mv_with_cleanup);
                Assert.NotNull(mv_without_cleanup);
                Assert.Equal(should_match, mv_without_cleanup.Query == mv_with_cleanup.Query);
            }
            AssertMaterializedView("mv", false);
            AssertMaterializedView("mv_preformatted", true);
        }
    }
}
