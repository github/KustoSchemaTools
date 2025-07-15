using Azure.Identity;
using Microsoft.Extensions.Logging;
using KustoSchemaTools.Model;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Factory for creating AAD object validators with different configurations
    /// </summary>
    public class AADValidatorFactory
    {
        /// <summary>
        /// Creates a GraphAADObjectValidator with the specified tenant ID
        /// </summary>
        /// <param name="logger">Logger instance</param>
        /// <param name="tenantId">Azure tenant ID (optional, defaults to GitHub tenant)</param>
        /// <returns>Configured AAD validator</returns>
        public static IAADObjectValidator CreateGraphValidator(ILogger<GraphAADObjectValidator> logger, string? tenantId = null)
        {
            return new GraphAADObjectValidator(logger, tenantId);
        }

        /// <summary>
        /// Creates a GraphAADObjectValidator using environment variables for configuration
        /// </summary>
        /// <param name="logger">Logger instance</param>
        /// <returns>Configured AAD validator</returns>
        public static IAADObjectValidator CreateGraphValidatorFromEnvironment(ILogger<GraphAADObjectValidator> logger)
        {
            var tenantId = Environment.GetEnvironmentVariable("AZURE_TENANT_ID") ?? 
                          Environment.GetEnvironmentVariable("AAD_TENANT_ID");
            
            return new GraphAADObjectValidator(logger, tenantId);
        }

        /// <summary>
        /// Creates a mock validator for testing purposes
        /// </summary>
        /// <param name="logger">Logger instance</param>
        /// <returns>Mock AAD validator that always returns valid results</returns>
        public static IAADObjectValidator CreateMockValidator(ILogger logger)
        {
            return new MockAADObjectValidator(logger);
        }
    }

    /// <summary>
    /// Mock AAD validator for testing and development purposes
    /// </summary>
    public class MockAADObjectValidator : IAADObjectValidator
    {
        private readonly ILogger _logger;

        public MockAADObjectValidator(ILogger logger)
        {
            _logger = logger;
        }

        public async Task<AADValidationResult> ValidateAADObjectAsync(AADObject aadObject)
        {
            _logger.LogDebug("Mock validation for AAD object: {Id}", aadObject?.Id);
            
            await Task.Delay(10); // Simulate async operation
            
            return new AADValidationResult
            {
                Id = aadObject?.Id ?? "unknown",
                Type = DetermineAADObjectType(aadObject?.Id ?? ""),
                IsValid = true,
                ValidatedAt = DateTime.UtcNow
            };
        }

        public async Task<IEnumerable<AADValidationResult>> ValidateAADObjectsAsync(IEnumerable<AADObject> aadObjects)
        {
            var validationTasks = aadObjects.Select(ValidateAADObjectAsync);
            return await Task.WhenAll(validationTasks);
        }

        public async Task<AADValidationResult> ValidateAADObjectByIdAsync(string id, AADObjectType type)
        {
            _logger.LogDebug("Mock validation for AAD object ID: {Id}, Type: {Type}", id, type);
            
            await Task.Delay(10); // Simulate async operation
            
            return new AADValidationResult
            {
                Id = id,
                Type = type,
                IsValid = true,
                ValidatedAt = DateTime.UtcNow
            };
        }

        private AADObjectType DetermineAADObjectType(string id)
        {
            if (id.StartsWith("aaduser=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.User;
            if (id.StartsWith("aadgroup=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.Group;
            if (id.StartsWith("aadapp=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.Application;
            
            return AADObjectType.Unknown;
        }
    }
}
