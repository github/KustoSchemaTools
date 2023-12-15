using KustoSchemaTools.Changes;
using KustoSchemaTools.Parser;
using System.Text;

namespace KustoSchemaTools.Model
{
    public class ExternalTable : IKustoBaseEntity
    {


        public Dictionary<string, string>? Schema { get; set; }
        public string Kind { get; set; } // sql, storage or delta
        public string? Folder { get; set; }
        public string? DocString { get; set; }
        public string? ConnectionString { get; set; }
        
        #region Storage Properties

        public string? Partitions { get; set; }
        public string? PathFormat { get; set; }
        public string? DataFormat { get; set; }
        public string? NamePrefix { get; set; }
        public string? Encoding { get; set; }

        // In the with()-block
        public string? FileExtensions { get; set; }
        public bool IncludeHeaders { get; set; }
        public bool Compressed { get; set; }

        #endregion
        #region SQL Properties
        public string? SqlTable { get; set; }
        public string? SqlDialect { get; set; }

        // In the with()-block
        public bool FireTriggres { get; set; }
        public bool CreateIfNotExists { get; set; }
        public string? PrimaryKey { get; set; }


        #endregion

        public List<DatabaseScriptContainer> CreateScripts(string name)
        {
            var container = new DatabaseScriptContainer
            {
                Kind = "External Table",
                Script = new DatabaseScript
                {
                    Order = 22,
                }
            };

            switch (Kind.ToLower())
            {
                case "delta":
                    container.Script.Text = CreateDeltaScript(name);
                    break;
                case "sql":
                    container.Script.Text = CreateSqlScript(name);
                    break;
                case "storage":
                     container.Script.Text = CreateStorageScript(name);
                    break;
                default:
                    throw new ArgumentException($"Kind {Kind} is not supported as external table");
            }

            return new List<DatabaseScriptContainer> { container };
        }
        private string CreateStorageScript(string name)
        {
            if (string.IsNullOrWhiteSpace(DataFormat)) throw new ArgumentException("DataFormat can't be empty");
            if (string.IsNullOrWhiteSpace(ConnectionString)) throw new ArgumentException("StorageConnectionString can't be empty");
            if (Schema?.Any() != true) throw new ArgumentException("Schema can't be empty");

            var sb = new StringBuilder();
            sb.AppendLine($".create-or-alter external table {name}");
            sb.AppendLine($"({string.Join(", ", Schema.Select(c => $"{c.Key.BracketIfIdentifier()}:{c.Value}"))})");
            sb.AppendLine("kind=storage");
            if (string.IsNullOrWhiteSpace(Partitions) == false)
            {
                sb.AppendLine($"partition by ({Partitions})");
            }
            if (string.IsNullOrWhiteSpace(PathFormat) == false)
            {
                sb.AppendLine($"pathformat=({Partitions})");
            }
            sb.AppendLine($"dataformat={DataFormat}");
            sb.AppendLine($"(h@'{ConnectionString}'");

            var ext = string.IsNullOrWhiteSpace(FileExtensions) ? "" : FileExtensions.StartsWith(".") ? FileExtensions : "." + FileExtensions;
            
            sb.Append($"with(folder='{Folder}', docString='{DocString}', fileExtension='{ext}', compressed={Compressed} ");
            if (IncludeHeaders)
            {
                sb.AppendLine(", includeHeaders=true");
            }
            if (string.IsNullOrWhiteSpace(Encoding) == false)
            {
                sb.Append($", encoding={Encoding}");
            }
            if (string.IsNullOrWhiteSpace(NamePrefix) == false)
            {
                sb.Append($", namePrefix={NamePrefix}");
            }
            sb.AppendLine(")");

            return sb.ToString();
        }

        private string CreateSqlScript(string name)
        {
            if (Schema?.Any() != true) throw new ArgumentException("Schema can't be empty");
            if (string.IsNullOrWhiteSpace(ConnectionString)) throw new ArgumentException("SqlConnectionString can't be empty");
            if (string.IsNullOrWhiteSpace(SqlTable)) throw new ArgumentException("SqlTable can't be empty");

            var sb = new StringBuilder();
            sb.AppendLine($".create-or-alter external table {name}");
            sb.AppendLine($"({string.Join(", ", Schema.Select(c => $"{c.Key.BracketIfIdentifier()}:{c.Value}"))})");
            sb.AppendLine("kind=sql");
            sb.AppendLine($"table={SqlTable}");
            sb.AppendLine($"({ConnectionString})");
            sb.Append($"with(folder='{Folder}', docString='{DocString}',createifnotexists={CreateIfNotExists}, fireTriggers={FireTriggres} ");
            if(CreateIfNotExists && string.IsNullOrWhiteSpace(PrimaryKey) == false)
            {
                sb.Append($", primaryKey='{PrimaryKey}'");
            }
            if(string.IsNullOrWhiteSpace(SqlDialect) == false)
            {
                sb.Append($", sqlDialect='{SqlDialect}'");
            }
            sb.AppendLine(")");

            return sb.ToString();
        }

        private string CreateDeltaScript(string name)
        {
            if (string.IsNullOrWhiteSpace(DataFormat)) throw new ArgumentException("DataFormat can't be empty");

            var sb = new StringBuilder();
            sb.AppendLine($".create-or-alter external table {name}");
            if (Schema?.Any() == true)
            {
                sb.AppendLine($"({string.Join(", ", Schema.Select(c => $"{c.Key.BracketIfIdentifier()}:{c.Value}"))})");
            }
            sb.AppendLine("kind=delta");            
            sb.AppendLine($"(h@'{ConnectionString}'");

            var ext = string.IsNullOrWhiteSpace(FileExtensions) ? "" : FileExtensions.StartsWith(".") ? FileExtensions : "." + FileExtensions;

            sb.AppendLine($"with(folder='{Folder}', docString='{DocString}', fileExtension='{ext}') ");

            return sb.ToString();
        }
    }
}
