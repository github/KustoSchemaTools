using KustoSchemaTools.Changes;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Tests.Changes
{
    public class ClusterGroupingTests
    {
        [Fact]
        public void BuildClusterFingerprint_IdenticalChanges_ProduceSameFingerprint()
        {
            var changes1 = CreateSampleChanges("## Table1\nSome diff");
            var changes2 = CreateSampleChanges("## Table1\nSome diff");
            var comments = new List<Comment>();

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes1, comments, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes2, comments, true);

            Assert.Equal(fp1, fp2);
        }

        [Fact]
        public void BuildClusterFingerprint_DifferentMarkdown_ProduceDifferentFingerprints()
        {
            var changes1 = CreateSampleChanges("## Table1\nDiff A");
            var changes2 = CreateSampleChanges("## Table1\nDiff B");
            var comments = new List<Comment>();

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes1, comments, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes2, comments, true);

            Assert.NotEqual(fp1, fp2);
        }

        [Fact]
        public void BuildClusterFingerprint_DifferentValidity_ProduceDifferentFingerprints()
        {
            var changes = CreateSampleChanges("## Table1\nSame diff");
            var comments = new List<Comment>();

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments, false);

            Assert.NotEqual(fp1, fp2);
        }

        [Fact]
        public void BuildClusterFingerprint_DifferentComments_ProduceDifferentFingerprints()
        {
            var changes = CreateSampleChanges("## Table1\nSame diff");
            var comments1 = new List<Comment>
            {
                new Comment { Kind = CommentKind.Warning, Text = "Warning 1", FailsRollout = false }
            };
            var comments2 = new List<Comment>
            {
                new Comment { Kind = CommentKind.Caution, Text = "Caution 1", FailsRollout = true }
            };

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments1, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments2, true);

            Assert.NotEqual(fp1, fp2);
        }

        [Fact]
        public void BuildClusterFingerprint_EmptyChanges_ProduceSameFingerprint()
        {
            var changes = new List<IChange>();
            var comments = new List<Comment>();

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments, true);

            Assert.Equal(fp1, fp2);
        }

        [Fact]
        public void BuildClusterFingerprint_CommentsInDifferentOrder_ProduceSameFingerprint()
        {
            var changes = CreateSampleChanges("## Table1\nSame diff");
            var comments1 = new List<Comment>
            {
                new Comment { Kind = CommentKind.Warning, Text = "A", FailsRollout = false },
                new Comment { Kind = CommentKind.Note, Text = "B", FailsRollout = false }
            };
            var comments2 = new List<Comment>
            {
                new Comment { Kind = CommentKind.Note, Text = "B", FailsRollout = false },
                new Comment { Kind = CommentKind.Warning, Text = "A", FailsRollout = false }
            };

            var fp1 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments1, true);
            var fp2 = KustoSchemaHandler<Database>.BuildClusterFingerprint(changes, comments2, true);

            Assert.Equal(fp1, fp2);
        }

        private static List<IChange> CreateSampleChanges(string markdown)
        {
            return new List<IChange>
            {
                new FakeChange(markdown)
            };
        }

        private class FakeChange : IChange
        {
            public FakeChange(string markdown)
            {
                Markdown = markdown;
            }

            public string EntityType => "Test";
            public string Entity => "TestEntity";
            public List<DatabaseScriptContainer> Scripts => new List<DatabaseScriptContainer>();
            public string Markdown { get; }
            public Comment Comment { get; set; }
        }
    }
}
