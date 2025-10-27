using System;

namespace KustoSchemaTools.Configuration
{
    /// <summary>
    /// Configuration settings for validation features in KustoSchemaTools.
    /// </summary>
    public class ValidationSettings
    {
        /// <summary>
        /// Gets or sets whether column order validation is enabled.
        /// Default is false to preserve existing behavior.
        /// </summary>
        public bool EnableColumnOrderValidation { get; set; } = false;

        /// <summary>
        /// Creates ValidationSettings from environment variables.
        /// </summary>
        /// <returns>A ValidationSettings instance configured from environment variables.</returns>
        public static ValidationSettings FromEnvironment()
        {
            var settings = new ValidationSettings();

            // Check for KUSTO_ENABLE_COLUMN_VALIDATION environment variable
            var enableColumnValidation = Environment.GetEnvironmentVariable("KUSTO_ENABLE_COLUMN_VALIDATION");
            if (!string.IsNullOrEmpty(enableColumnValidation))
            {
                // Try standard boolean parsing first
                if (bool.TryParse(enableColumnValidation, out bool enable))
                {
                    settings.EnableColumnOrderValidation = enable;
                }
                // Also accept "1" as true and "0" as false
                else if (enableColumnValidation == "1")
                {
                    settings.EnableColumnOrderValidation = true;
                }
                else if (enableColumnValidation == "0")
                {
                    settings.EnableColumnOrderValidation = false;
                }
            }

            return settings;
        }

        /// <summary>
        /// Creates ValidationSettings with column order validation enabled.
        /// </summary>
        /// <returns>A ValidationSettings instance with column order validation enabled.</returns>
        public static ValidationSettings WithColumnOrderValidation()
        {
            return new ValidationSettings
            {
                EnableColumnOrderValidation = true
            };
        }
    }
}
