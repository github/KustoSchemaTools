using CommandLine;

namespace KustoSchemaTools.Cli
{

    public class ActionInputs
    {

        [Option('m', "mode",
            Required = true,
            HelpText = "The Mode how the action should work: diff, import, apply")]
        public string Mode { get; set; }
        [Option('p', "path",
            Required = true,
            HelpText = "The base path for all Kusto configurations")]
        public string Path { get; set; }
        [Option('c', "cluster",
            Required = true,
            HelpText = "The cluster")]
        public string Cluster { get; set; }
        [Option('d', "db",
            Required = true,
            HelpText = "The database")]
        public string Database { get; set; }
        [Option('i', "includeColumns",
            Required = false,
            HelpText = "Include columns on import")]
        public string IncludeColumns { get; set; }
        [Option('o', "operationsCluster",
            Required = true,
            HelpText = "The cluster that is used for evaluating all kinds of things")]
        public string OperationsCluster { get; set; }

    }
}
