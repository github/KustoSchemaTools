using Kusto.Data;
using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;
using KustoSchemaTools.Parser;
using KustoSchemaTools.Parser.KustoWriter;
using Microsoft.Extensions.Logging;
using Moq;

namespace KustoSchemaTools.Tests
{
    public class DefaultDatabaseWriterTests
    {
        [Fact]
        public async Task UpdatePrimary_ShouldRetryUntilChangesSucceed()
        {
            // Arrange
            var sourceDb = new Database { Name = "SourceDb" };
            var targetDb = new Database { Name = "TargetDb" };

            var logger = new ConsoleLogger();
            var mockClient = new Mock<KustoClient>(MockBehavior.Loose, "test");

            var writer = new TestableDefaultDatabaseWriter();

            // Create mock changes for the test
            writer.Setup(true, false, false);
            writer.Setup(true, true);

            // Act
            await writer.WriteAsync(sourceDb, targetDb, mockClient.Object, logger);
            Assert.Equal(2, writer.GenerateChangesCallCount);
            Assert.Equal(2, writer.ApplyChangesToDatabaseCallCount);
            Assert.Equal(0, writer.ExceptionCount);
        }

        [Fact]
        public async Task UpdatePrimary_NoRetryIfAllSuccess()
        {
            // Arrange
            var sourceDb = new Database { Name = "SourceDb" };
            var targetDb = new Database { Name = "TargetDb" };
            var logger = new ConsoleLogger();
            var mockClient = new Mock<KustoClient>(MockBehavior.Loose, "test");
            var writer = new TestableDefaultDatabaseWriter();

            // Create mock changesets for the test
            writer.Setup(true, true, true);
            writer.Setup(true, true, true);

            // all success
            await writer.WriteAsync(sourceDb, targetDb, mockClient.Object, logger);
            Assert.Equal(1, writer.GenerateChangesCallCount);
            Assert.Equal(1, writer.ApplyChangesToDatabaseCallCount);
            Assert.Equal(0, writer.ExceptionCount);
        }

        [Fact]
        public async Task UpdatePrimary_NoRetryIfAllFail()
        {
            // Arrange
            var sourceDb = new Database { Name = "SourceDb" };
            var targetDb = new Database { Name = "TargetDb" };
            var logger = new ConsoleLogger();
            var mockClient = new Mock<KustoClient>(MockBehavior.Loose, "test");
            var writer = new TestableDefaultDatabaseWriter();

            // Create mock changesets for the test
            writer.Setup(false, false, false);
            writer.Setup(false, false, false);

            await writer.WriteAsync(sourceDb, targetDb, mockClient.Object, logger);
            Assert.Equal(1, writer.GenerateChangesCallCount);
            Assert.Equal(1, writer.ApplyChangesToDatabaseCallCount);
            Assert.Equal(1, writer.ExceptionCount);
        }


        [Fact]
        public async Task UpdatePrimary_ShouldRetryUntilNoSuccessfulChanges()
        {
            // Arrange
            var sourceDb = new Database { Name = "SourceDb" };
            var targetDb = new Database { Name = "TargetDb" };
            var logger = new ConsoleLogger();
            var mockClient = new Mock<KustoClient>(MockBehavior.Loose, "test");
            var writer = new TestableDefaultDatabaseWriter();

            // Create mock changes for the test
            writer.Setup(true, false, false);
            writer.Setup(true, false);
            writer.Setup(false);
            writer.Setup(false); // one extra

            // Act
            await writer.WriteAsync(sourceDb, targetDb, mockClient.Object, logger);
            Assert.Equal(3, writer.GenerateChangesCallCount);
            Assert.Equal(3, writer.ApplyChangesToDatabaseCallCount);
            Assert.Equal(1, writer.ExceptionCount);
        }

        #region Helper Methods


        static IChange CreateMockChange(string name)
        {
            var mockChange = new Mock<IChange>();

            var scripts = new List<DatabaseScriptContainer> {
                new DatabaseScriptContainer(
                    new DatabaseScript(name, 0),
                    "TestKind"
                )
            };

            mockChange.Setup(c => c.Scripts).Returns(scripts);
            return mockChange.Object;
        }

        static ScriptExecuteCommandResult CreateMockResult(IChange change, bool isSuccess)
        {
            return new ScriptExecuteCommandResult
            {
                OperationId = Guid.NewGuid(),
                CommandType = "Script",
                Result = isSuccess ? "Completed" : "Failed",
                CommandText = change.Scripts.First().Script.Text,
                Reason = isSuccess ? null : "Test failure"
            };
        }

        #endregion

        #region Test Support Classes

        /// <summary>
        /// A testable version of DefaultDatabaseWriter that we can use to track calls and control behavior
        /// </summary>
        private class TestableDefaultDatabaseWriter : DefaultDatabaseWriter
        {
            public Dictionary<IChange, ScriptExecuteCommandResult> ResultsCache { get; } = new Dictionary<IChange, ScriptExecuteCommandResult>();
            private readonly IList<List<IChange>> _generateChangesResults = new List<List<IChange>>();
            public int GenerateChangesCallCount { get; private set; } = 0;
            public int ApplyChangesToDatabaseCallCount { get; private set; } = 0;
            public int ExceptionCount { get; private set; } = 0;

            public void ResetCounts()
            {
                GenerateChangesCallCount = ApplyChangesToDatabaseCallCount = ExceptionCount = 0;
            }

            public void Setup(params bool[] results)
            {
                var changeSet = new List<IChange> { };
                foreach (var result in results)
                {
                    var change = CreateMockChange($"change");
                    var scriptExecuteResult = CreateMockResult(change, result);
                    ResultsCache.Add(change, scriptExecuteResult);
                    changeSet.Add(change);
                }
                _generateChangesResults.Add(changeSet);
            }

            // Override the base method to use our predetermined results
            internal override List<IChange> GenerateChanges(Database targetDb, Database sourceDb, ILogger logger)
            {
                if (GenerateChangesCallCount >= _generateChangesResults.Count)
                {
                    throw new InvalidOperationException("No more predefined change sets available.");
                }

                var changes = _generateChangesResults.ElementAt(GenerateChangesCallCount);
                GenerateChangesCallCount++;
                return changes;
            }

            // Override the protected method to return our predetermined results
            internal override Task<List<ScriptExecuteCommandResult>> ApplyChangesToDatabase(string databaseName, List<IChange> changes, KustoClient client, ILogger logger)
            {
                ApplyChangesToDatabaseCallCount++;
                var results = new List<ScriptExecuteCommandResult>();
                foreach (var c in changes)
                {
                    var result = ResultsCache[c];
                    results.Add(result);
                }
                return Task.FromResult(results);
            }

            // Expose the internal UpdatePrimary method for testing
            public Task<List<ScriptExecuteCommandResult>> TestUpdatePrimary(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
            {
                return UpdatePrimary(sourceDb, targetDb, client, logger);
            }

            // Override WriteAsync to control exception throwing
            public override async Task WriteAsync(Database sourceDb, Database targetDb, KustoClient client, ILogger logger)
            {
                try
                {
                    await base.WriteAsync(sourceDb, targetDb, client, logger);
                }
                catch (Exception)
                {
                    ExceptionCount++;
                }
            }
        }

        // Custom console logger implementation
        private class ConsoleLogger : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => new DummyDisposable();
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                Console.WriteLine($"[{logLevel}] {formatter(state, exception)}");
                if (exception != null)
                {
                    Console.WriteLine($"Exception: {exception.Message}");
                }
            }

            private class DummyDisposable : IDisposable
            {
                public void Dispose() { }
            }
        }
        #endregion
    }
}
