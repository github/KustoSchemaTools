namespace KustoSchemaTools.KustoTypes
{
    public class ShowDatabase
    {

        public string DatabaseName { get; set; }
        public string PersistentStorage { get; set; }
        public string Version { get; set; }
        public bool IsCurrent { get; set; }
        public string DatabaseAccessMode { get; set; }
        public string PrettyName { get; set; }
        public bool ReservedSlot1 { get; set; }
        public Guid DatabaseId { get; set; }
        public string InTransitionTo { get; set; }
        public string SuspensionState { get; set; }

    }
}
