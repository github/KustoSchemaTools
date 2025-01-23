namespace KustoSchemaTools.Changes
{
    public class Comment
    {
        public string Text { get; set; }
        public bool FailsRollout { get; set; }
        public CommentKind Kind { get; set; }

    }
}