using KustoSchemaTools.Model;
using KustoSchemaTools.Plugins;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Plugin that validates all AAD objects referenced in the database schema
    /// This plugin runs after the database is loaded and validates all AAD objects
    /// </summary>
    public class AADValidationPlugin : IYamlSchemaPlugin<Database>
    {
        private readonly IAADObjectValidator _aadValidator;
        private readonly ILogger<AADValidationPlugin> _logger;

        /// <summary>
        /// Constructor that accepts dependencies for manual injection
        /// </summary>
        public AADValidationPlugin(IAADObjectValidator aadValidator, ILogger<AADValidationPlugin> logger)
        {
            _aadValidator = aadValidator;
            _logger = logger;
        }

        /// <summary>
        /// Constructor for simple creation without dependency injection - uses mock validator
        /// </summary>
        public AADValidationPlugin()
        {
            var loggerFactory = LoggerFactory.Create(builder => { });
            _logger = loggerFactory.CreateLogger<AADValidationPlugin>();
            
            // Use environment variable to determine if real validation should be used
            var useMockValidation = Environment.GetEnvironmentVariable("USE_MOCK_AAD_VALIDATION")?.ToLower() != "false";
            
            if (useMockValidation)
            {
                var mockLogger = loggerFactory.CreateLogger<MockAADObjectValidator>();
                _aadValidator = AADValidatorFactory.CreateMockValidator(mockLogger);
                _logger.LogInformation("Using mock AAD validation (set USE_MOCK_AAD_VALIDATION=false to use real validation)");
            }
            else
            {
                var graphLogger = loggerFactory.CreateLogger<GraphAADObjectValidator>();
                _aadValidator = AADValidatorFactory.CreateGraphValidatorFromEnvironment(graphLogger);
                _logger.LogInformation("Using real AAD validation via Microsoft Graph");
            }
        }

        public async Task OnLoad(Database existingDatabase, string basePath)
        {
            _logger.LogInformation("Starting AAD object validation for database {DatabaseName}", existingDatabase.Name);

            // Collect all AAD objects from the database
            var aadObjects = CollectAADObjects(existingDatabase);

            if (!aadObjects.Any())
            {
                _logger.LogInformation("No AAD objects found to validate");
                return;
            }

            _logger.LogInformation("Found {Count} AAD objects to validate", aadObjects.Count);

            // Validate all AAD objects in parallel
            var validationResults = await _aadValidator.ValidateAADObjectsAsync(aadObjects);

            // Process validation results
            await ProcessValidationResults(validationResults.ToList(), existingDatabase);
        }

        public Task OnWrite(Database existingDatabase, string basePath)
        {
            // No action needed on write
            return Task.CompletedTask;
        }

        /// <summary>
        /// Collects all AAD objects referenced in the database
        /// </summary>
        private List<AADObject> CollectAADObjects(Database database)
        {
            var aadObjects = new List<AADObject>();

            // Database-level permissions
            aadObjects.AddRange(database.Admins ?? new List<AADObject>());
            aadObjects.AddRange(database.Users ?? new List<AADObject>());
            aadObjects.AddRange(database.Viewers ?? new List<AADObject>());
            aadObjects.AddRange(database.UnrestrictedViewers ?? new List<AADObject>());
            aadObjects.AddRange(database.Ingestors ?? new List<AADObject>());
            aadObjects.AddRange(database.Monitors ?? new List<AADObject>());

            // Collect AAD objects from policies (row-level security, etc.)
            foreach (var table in database.Tables?.Values ?? Enumerable.Empty<Table>())
            {
                aadObjects.AddRange(ExtractAADObjectsFromRowLevelSecurity(table.Policies?.RowLevelSecurity));
            }

            foreach (var mv in database.MaterializedViews?.Values ?? Enumerable.Empty<MaterializedView>())
            {
                aadObjects.AddRange(ExtractAADObjectsFromRowLevelSecurity(mv.Policies?.RowLevelSecurity));
            }

            // Remove duplicates based on ID
            return aadObjects.GroupBy(obj => obj.Id).Select(g => g.First()).ToList();
        }

        /// <summary>
        /// Extracts AAD object references from row-level security policies
        /// This is a simplified implementation - in reality, you might need more sophisticated parsing
        /// </summary>
        private List<AADObject> ExtractAADObjectsFromRowLevelSecurity(string? rowLevelSecurity)
        {
            var aadObjects = new List<AADObject>();

            if (string.IsNullOrWhiteSpace(rowLevelSecurity))
                return aadObjects;

            // Look for patterns like "aaduser=", "aadgroup=", "aadapp=" in the RLS policy
            var patterns = new[] { "aaduser=", "aadgroup=", "aadapp=" };

            foreach (var pattern in patterns)
            {
                var startIndex = 0;
                while ((startIndex = rowLevelSecurity.IndexOf(pattern, startIndex, StringComparison.OrdinalIgnoreCase)) != -1)
                {
                    startIndex += pattern.Length;
                    
                    // Extract the AAD object ID/name until we hit a delimiter
                    var endIndex = FindEndOfAADId(rowLevelSecurity, startIndex);
                    if (endIndex > startIndex)
                    {
                        var aadId = rowLevelSecurity.Substring(startIndex, endIndex - startIndex);
                        if (!string.IsNullOrWhiteSpace(aadId))
                        {
                            aadObjects.Add(new AADObject
                            {
                                Id = $"{pattern}{aadId}",
                                Name = aadId // We'll get the proper name during validation
                            });
                        }
                    }
                    
                    startIndex = endIndex;
                }
            }

            return aadObjects;
        }

        /// <summary>
        /// Finds the end of an AAD ID in a policy string
        /// </summary>
        private int FindEndOfAADId(string text, int startIndex)
        {
            var delimiters = new[] { '"', '\'', ',', ')', ']', ' ', '\t', '\r', '\n' };
            
            for (int i = startIndex; i < text.Length; i++)
            {
                if (delimiters.Contains(text[i]))
                {
                    return i;
                }
            }
            
            return text.Length;
        }

        /// <summary>
        /// Processes validation results and logs issues
        /// </summary>
        private Task ProcessValidationResults(List<AADValidationResult> results, Database database)
        {
            var validResults = results.Where(r => r.IsValid).ToList();
            var invalidResults = results.Where(r => !r.IsValid).ToList();

            _logger.LogInformation("AAD validation completed: {ValidCount} valid, {InvalidCount} invalid", 
                validResults.Count, invalidResults.Count);

            foreach (var invalid in invalidResults)
            {
                _logger.LogWarning("Invalid AAD object: {Id} - {Error}", invalid.Id, invalid.ErrorMessage);
            }

            // You could also add metadata to the database about validation results
            if (invalidResults.Any())
            {
                var metadata = new Metadata
                {
                    EntityName = database.Name,
                    EntityType = "Database",
                    Type = "AADValidation",
                    Value = $"Found {invalidResults.Count} invalid AAD objects"
                };

                database.Metadata?.Add(metadata);
            }

            return Task.CompletedTask;
        }
    }
}
