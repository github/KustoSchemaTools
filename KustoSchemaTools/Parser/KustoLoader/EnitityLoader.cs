namespace KustoSchemaTools.Parser.KustoLoader
{
    public class EnitityLoader<T>
    {
        public string EntityName { get; set; }
        public string EntityType { get; set; }
        public T Body { get; set; }
    }
}
