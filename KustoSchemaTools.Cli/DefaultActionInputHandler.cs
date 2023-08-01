using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Text;

namespace KustoSchemaTools.Cli
{
    public static class DefaultActionInputHandler
    {
        public static async ValueTask StartAnalysisAsync(ActionInputs options, KustoSchemaHandler handler, ILogger logger)
        {
            logger.Log(LogLevel.Information, $"Received following input: {JsonConvert.SerializeObject(options)}");

            try
            {

                var githubOutputFile = Environment.GetEnvironmentVariable("GITHUB_OUTPUT", EnvironmentVariableTarget.Process);

                switch (options.Mode)
                {
                    case "diff":
                        var response = await handler.GenerateDiffMarkdown(Path.Combine(options.Path, options.Cluster), options.Database, options.OperationsCluster);
                        var json = JsonConvert.SerializeObject(new { response.markDown, response.isValid });
                        logger.Log(LogLevel.Information, $"Sending following output: {json}");
                        if (!string.IsNullOrWhiteSpace(githubOutputFile))
                        {
                            logger.LogInformation($"Using the output file: {githubOutputFile}");
                            using (var textWriter = new StreamWriter(githubOutputFile!, true, Encoding.UTF8))
                            {
                                textWriter.WriteLine($"diff={json}");
                                textWriter.Flush();
                            }
                        }
                        else
                        {
                            logger.LogInformation("Writing to console");

                            Console.WriteLine($"::set-output name=diff::{json}");
                        }

                        Environment.Exit(response.isValid ? 0 : 2);
                        break;
                    case "import":
                        await handler.Import(Path.Combine(options.Path, options.Cluster), options.Database, options.OperationsCluster, options.IncludeColumns.ToLower() == "true");
                        break;
                    case "apply":
                        await handler.Apply(Path.Combine(options.Path, options.Cluster), options.Database, options.OperationsCluster);
                        break;
                    case "debug":
                        var debugJson = JsonConvert.SerializeObject(new { markDown = "# This is a heading\n\n and some more text", isValid = options.IncludeColumns });
                        logger.Log(LogLevel.Information, $"Sending following output: {debugJson}");
                        if (!string.IsNullOrWhiteSpace(githubOutputFile))
                        {
                            using (var textWriter = new StreamWriter(githubOutputFile!, true, Encoding.UTF8))
                            {
                                logger.LogInformation($"Using the output file: {githubOutputFile}");
                                textWriter.WriteLine($"diff={debugJson}");
                                textWriter.Flush();
                            }
                        }
                        else
                        {
                            logger.LogInformation("Writing to console");
                            Console.WriteLine($"::set-output name=diff::{debugJson}");
                        }

                        Environment.Exit(options.IncludeColumns.ToLower() == "true" ? 0 : 2);
                        break;
                }
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to run the action");
                Environment.Exit(2);
            }

            Environment.Exit(0);
        }
    }
}
