using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using KustoSchemaTools.Model;
using System.Text.RegularExpressions;

namespace KustoSchemaTools.Validation
{
    /// <summary>
    /// Implementation of AAD object validator using Microsoft Graph API
    /// </summary>
    public class GraphAADObjectValidator : IAADObjectValidator
    {
        private readonly GraphServiceClient _graphClient;
        private readonly ILogger<GraphAADObjectValidator> _logger;
        private readonly string _tenantId;

        public GraphAADObjectValidator(ILogger<GraphAADObjectValidator> logger, string? tenantId = null)
        {
            _logger = logger;
            _tenantId = tenantId ?? "398a6654-997b-47e9-b12b-9515b896b4de"; // Default GitHub tenant ID

            // Create authentication provider using Azure Identity
            var credential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
            {
                TenantId = _tenantId
            });

            _graphClient = new GraphServiceClient(credential, new[] { "https://graph.microsoft.com/.default" });
        }

        public async Task<AADValidationResult> ValidateAADObjectAsync(AADObject aadObject)
        {
            if (aadObject == null)
            {
                return new AADValidationResult
                {
                    IsValid = false,
                    ErrorMessage = "AAD object is null",
                    ValidatedAt = DateTime.UtcNow
                };
            }

            // Determine the type of AAD object based on the ID format
            var objectType = DetermineAADObjectType(aadObject.Id);
            return await ValidateAADObjectByIdAsync(aadObject.Id, objectType);
        }

        public async Task<IEnumerable<AADValidationResult>> ValidateAADObjectsAsync(IEnumerable<AADObject> aadObjects)
        {
            if (aadObjects == null)
            {
                return new List<AADValidationResult>();
            }

            var validationTasks = aadObjects.Select(ValidateAADObjectAsync);
            return await Task.WhenAll(validationTasks);
        }

        public async Task<AADValidationResult> ValidateAADObjectByIdAsync(string id, AADObjectType type)
        {
            var result = new AADValidationResult
            {
                Id = id,
                Type = type,
                ValidatedAt = DateTime.UtcNow
            };

            try
            {
                if (string.IsNullOrWhiteSpace(id))
                {
                    result.IsValid = false;
                    result.ErrorMessage = "ID is null or empty";
                    return result;
                }

                // Clean the ID - remove any prefixes like "aaduser=", "aadgroup=", etc.
                var cleanId = CleanAADId(id);
                result.Id = cleanId;

                switch (type)
                {
                    case AADObjectType.User:
                        return await ValidateUserAsync(cleanId, result);
                    case AADObjectType.Group:
                        return await ValidateGroupAsync(cleanId, result);
                    case AADObjectType.Application:
                        return await ValidateApplicationAsync(cleanId, result);
                    case AADObjectType.ServicePrincipal:
                        return await ValidateServicePrincipalAsync(cleanId, result);
                    default:
                        // Try to auto-detect the type
                        return await ValidateUnknownTypeAsync(cleanId, result, id); // Pass original id
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating AAD object {Id} of type {Type}", id, type);
                result.IsValid = false;
                result.ErrorMessage = $"Validation failed: {ex.Message}";
                result.Exists = false;
                return result;
            }
        }

        private async Task<AADValidationResult> ValidateUserAsync(string id, AADValidationResult result)
        {
            try
            {
                var user = await _graphClient.Users[id].GetAsync();
                if (user != null)
                {
                    result.IsValid = true;
                    result.Exists = true;
                    result.Name = user.DisplayName ?? user.UserPrincipalName ?? id;
                    result.Type = AADObjectType.User;
                    _logger.LogDebug("User {Id} found: {Name}", id, result.Name);
                }
                else
                {
                    result.IsValid = false;
                    result.Exists = false;
                    result.ErrorMessage = "User not found";
                }
            }
            catch (Microsoft.Graph.Models.ODataErrors.ODataError odataError) when (odataError.Error?.Code == "Request_ResourceNotFound")
            {
                result.IsValid = false;
                result.Exists = false;
                result.ErrorMessage = "User does not exist in the tenant";
            }

            return result;
        }

        private async Task<AADValidationResult> ValidateGroupAsync(string id, AADValidationResult result)
        {
            try
            {
                var group = await _graphClient.Groups[id].GetAsync();
                if (group != null)
                {
                    result.IsValid = true;
                    result.Exists = true;
                    result.Name = group.DisplayName ?? id;
                    result.Type = AADObjectType.Group;
                    _logger.LogDebug("Group {Id} found: {Name}", id, result.Name);
                }
                else
                {
                    result.IsValid = false;
                    result.Exists = false;
                    result.ErrorMessage = "Group not found";
                }
            }
            catch (Microsoft.Graph.Models.ODataErrors.ODataError odataError) when (odataError.Error?.Code == "Request_ResourceNotFound")
            {
                result.IsValid = false;
                result.Exists = false;
                result.ErrorMessage = "Group does not exist in the tenant";
            }

            return result;
        }

        private async Task<AADValidationResult> ValidateApplicationAsync(string id, AADValidationResult result)
        {
            try
            {
                var application = await _graphClient.Applications[id].GetAsync();
                if (application != null)
                {
                    result.IsValid = true;
                    result.Exists = true;
                    result.Name = application.DisplayName ?? id;
                    result.Type = AADObjectType.Application;
                    _logger.LogDebug("Application {Id} found: {Name}", id, result.Name);
                }
                else
                {
                    result.IsValid = false;
                    result.Exists = false;
                    result.ErrorMessage = "Application not found";
                }
            }
            catch (Microsoft.Graph.Models.ODataErrors.ODataError odataError) when (odataError.Error?.Code == "Request_ResourceNotFound")
            {
                result.IsValid = false;
                result.Exists = false;
                result.ErrorMessage = "Application does not exist in the tenant";
            }

            return result;
        }

        private async Task<AADValidationResult> ValidateServicePrincipalAsync(string id, AADValidationResult result)
        {
            try
            {
                // First try direct lookup by Service Principal Object ID
                var servicePrincipal = await _graphClient.ServicePrincipals[id].GetAsync();
                if (servicePrincipal != null)
                {
                    result.IsValid = true;
                    result.Exists = true;
                    result.Name = servicePrincipal.DisplayName ?? id;
                    result.Type = AADObjectType.ServicePrincipal;
                    _logger.LogDebug("Service Principal {Id} found by Object ID: {Name}", id, result.Name);
                    return result;
                }
            }
            catch (Microsoft.Graph.Models.ODataErrors.ODataError odataError) when (odataError.Error?.Code == "Request_ResourceNotFound")
            {
                // If not found by Object ID, try to find by Application ID
                try
                {
                    var servicePrincipals = await _graphClient.ServicePrincipals
                        .GetAsync(requestConfiguration => 
                        {
                            requestConfiguration.QueryParameters.Filter = $"appId eq '{id}'";
                            requestConfiguration.QueryParameters.Top = 1;
                        });

                    var servicePrincipal = servicePrincipals?.Value?.FirstOrDefault();
                    if (servicePrincipal != null)
                    {
                        result.IsValid = true;
                        result.Exists = true;
                        result.Name = servicePrincipal.DisplayName ?? id;
                        result.Type = AADObjectType.ServicePrincipal;
                        _logger.LogDebug("Service Principal {Id} found by Application ID: {Name}", id, result.Name);
                        return result;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogDebug("Failed to query Service Principal by Application ID {Id}: {Error}", id, ex.Message);
                }

                result.IsValid = false;
                result.Exists = false;
                result.ErrorMessage = "Service Principal does not exist in the tenant";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error validating Service Principal {Id}", id);
                result.IsValid = false;
                result.Exists = false;
                result.ErrorMessage = $"Service Principal validation failed: {ex.Message}";
            }

            return result;
        }

        private async Task<AADValidationResult> ValidateUnknownTypeAsync(string id, AADValidationResult result, string? originalId = null)
        {
            // Determine the order to try types based on the original ID format
            var idToCheck = originalId ?? id;
            var types = new AADObjectType[] { AADObjectType.User, AADObjectType.Group, AADObjectType.Application, AADObjectType.ServicePrincipal };
            
            // For aadapp= prefixes, try ServicePrincipal first as it's more common in Kusto
            if (idToCheck.StartsWith("aadapp=", StringComparison.OrdinalIgnoreCase))
            {
                types = new[] { AADObjectType.ServicePrincipal, AADObjectType.Application, AADObjectType.User, AADObjectType.Group };
            }

            foreach (var type in types)
            {
                var tempResult = new AADValidationResult { Id = id, Type = type, ValidatedAt = DateTime.UtcNow };
                
                try
                {
                    switch (type)
                    {
                        case AADObjectType.User:
                            tempResult = await ValidateUserAsync(id, tempResult);
                            break;
                        case AADObjectType.Group:
                            tempResult = await ValidateGroupAsync(id, tempResult);
                            break;
                        case AADObjectType.Application:
                            tempResult = await ValidateApplicationAsync(id, tempResult);
                            break;
                        case AADObjectType.ServicePrincipal:
                            tempResult = await ValidateServicePrincipalAsync(id, tempResult);
                            break;
                    }

                    if (tempResult.IsValid && tempResult.Exists)
                    {
                        return tempResult;
                    }
                }
                catch
                {
                    // Continue to next type
                    continue;
                }
            }

            // If we get here, none of the types worked
            result.IsValid = false;
            result.Exists = false;
            result.ErrorMessage = "AAD object not found as user, group, application, or service principal";
            result.Type = AADObjectType.Unknown;

            return result;
        }

        private static AADObjectType DetermineAADObjectType(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return AADObjectType.Unknown;

            var cleanId = CleanAADId(id);

            // Check for specific prefixes in the original ID
            if (id.StartsWith("aaduser=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.User;
            if (id.StartsWith("aadgroup=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.Group;
            if (id.StartsWith("aadapp=", StringComparison.OrdinalIgnoreCase))
                return AADObjectType.Unknown; // aadapp= can be either Application or ServicePrincipal, so try both

            // Check format patterns
            if (IsEmail(cleanId))
                return AADObjectType.User;
            if (IsGuid(cleanId))
                return AADObjectType.Unknown; // Could be any type, will need to try all

            return AADObjectType.Unknown;
        }

        private static string CleanAADId(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return id;

            // Remove common prefixes
            var prefixes = new[] { "aaduser=", "aadgroup=", "aadapp=", "aadprincipal=" };
            foreach (var prefix in prefixes)
            {
                if (id.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                {
                    id = id.Substring(prefix.Length);
                    break;
                }
            }

            // Handle cases where there might be a semicolon followed by tenant info
            var semicolonIndex = id.IndexOf(';');
            if (semicolonIndex > 0)
            {
                id = id.Substring(0, semicolonIndex);
            }

            return id.Trim();
        }

        private static bool IsEmail(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                return false;

            try
            {
                var emailRegex = new Regex(@"^[^@\s]+@[^@\s]+\.[^@\s]+$", RegexOptions.IgnoreCase);
                return emailRegex.IsMatch(input);
            }
            catch
            {
                return false;
            }
        }

        private static bool IsGuid(string input)
        {
            return Guid.TryParse(input, out _);
        }
    }
}
