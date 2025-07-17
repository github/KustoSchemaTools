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
            
            // Simulate failure for obviously invalid objects OR invalid formats
            var hasInvalidContent = (aadObject?.Id?.Contains("invalid-domain.com") == true ||
                                   aadObject?.Id?.Contains("definitely-invalid") == true);
            var hasValidFormat = IsValidAADFormat(aadObject?.Id);
            var isValidMockPrincipal = !hasInvalidContent && hasValidFormat;
            
            return new AADValidationResult
            {
                Id = aadObject?.Id ?? "unknown",
                Type = DetermineAADObjectType(aadObject?.Id ?? ""),
                IsValid = isValidMockPrincipal,
                ErrorMessage = isValidMockPrincipal ? "" : "Mock validation: Principal appears to be invalid or has invalid format",
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
            
            // For ById validation, we only check for obviously invalid content, not format
            // since the ID part doesn't include the aaduser=/aadgroup=/aadapp= prefix
            var isValidMockPrincipal = !(id?.Contains("invalid-domain.com") == true ||
                                       id?.Contains("definitely-invalid") == true) &&
                                       !string.IsNullOrWhiteSpace(id);
            
            return new AADValidationResult
            {
                Id = id ?? "unknown",
                Type = type,
                IsValid = isValidMockPrincipal,
                ErrorMessage = isValidMockPrincipal ? "" : "Mock validation: Principal appears to be invalid or has invalid format",
                ValidatedAt = DateTime.UtcNow
            };
        }

        private bool IsValidAADFormat(string? id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return false;

            // Valid formats per Microsoft documentation:
            // https://learn.microsoft.com/en-us/kusto/management/reference-security-principals#referencing-microsoft-entra-principals-and-groups
            
            // User formats:
            // - aaduser=UPN (implicit)
            // - aaduser=UPN;TenantId or aaduser=ObjectID;TenantId (explicit with ID)
            // - aaduser=UPN;TenantName or aaduser=ObjectID;TenantName (explicit with name)
            if (id.StartsWith("aaduser=", StringComparison.OrdinalIgnoreCase))
            {
                var userPart = id.Substring(8); // Remove "aaduser=" prefix
                // User can be implicit (no semicolon) or explicit (with semicolon and tenant)
                return !string.IsNullOrWhiteSpace(userPart);
            }

            // Group formats:
            // - aadgroup=GroupEmailAddress (implicit)
            // - aadgroup=GroupDisplayName;TenantId or aadgroup=GroupObjectId;TenantId (explicit with ID)
            // - aadgroup=GroupDisplayName;TenantName or aadgroup=GroupObjectId;TenantName (explicit with name)
            if (id.StartsWith("aadgroup=", StringComparison.OrdinalIgnoreCase))
            {
                var groupPart = id.Substring(9); // Remove "aadgroup=" prefix
                // Group can be implicit (no semicolon) or explicit (with semicolon and tenant)
                return !string.IsNullOrWhiteSpace(groupPart);
            }

            // App formats:
            // - aadapp=ApplicationDisplayName;TenantId or aadapp=ApplicationId;TenantId (explicit with ID)
            // - aadapp=ApplicationDisplayName;TenantName or aadapp=ApplicationId;TenantName (explicit with name)
            // NOTE: Apps have NO implicit format - must always specify tenant
            if (id.StartsWith("aadapp=", StringComparison.OrdinalIgnoreCase))
            {
                var appPart = id.Substring(7); // Remove "aadapp=" prefix
                // App MUST have a semicolon (tenant is required)
                return !string.IsNullOrWhiteSpace(appPart) && appPart.Contains(';');
            }

            // Unknown format
            return false;
        }

        private AADObjectType DetermineAADObjectType(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return AADObjectType.Unknown;

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
