﻿namespace KustoSchemaTools.Model
{
    public class Database
    {
        public string Name { get; set; }
        public string Team { get; set; } = "";
        public RetentionAndCachePolicy DefaultRetentionAndCache { get; set; } = new RetentionAndCachePolicy();

        public List<AADObject> Monitors { get; set; } = new List<AADObject>();

        public List<AADObject> Viewers { get; set; } = new List<AADObject>();
        public List<AADObject> UnrestrictedViewers { get; set; } = new List<AADObject>();
        public List<AADObject> Users { get; set; } = new List<AADObject>();
        public List<AADObject> Ingestors { get; set; } = new List<AADObject>();
        public List<AADObject> Admins { get; set; } = new List<AADObject>();

        public Dictionary<string, Table> Tables { get; set; } = new Dictionary<string, Table>();

        public Dictionary<string, MaterializedView> MaterializedViews { get; set; } = new Dictionary<string, MaterializedView>();

        public Dictionary<string, Function> Functions { get; set; } = new Dictionary<string, Function>();

        public List<DatabaseScript> Scripts { get; set; } = new List<DatabaseScript>();

    }

}
