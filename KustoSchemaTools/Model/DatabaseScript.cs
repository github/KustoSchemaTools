namespace KustoSchemaTools.Model
{
    public class DatabaseScript
    {
        public DatabaseScript(string text, int order)
        {
            Text = text;
            Order = order;
        }

        public DatabaseScript()
        {
        }

        public string Text { get; set; }
        public int Order { get; set; }
    }

}
