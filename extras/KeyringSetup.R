# =============================================================================
# setup_keyring.R
# Run this script ONCE interactively to securely store credentials.
# Never run this in an automated/non-interactive pipeline.
# =============================================================================

# Install keyring if not already installed
if (!requireNamespace("keyring", quietly = TRUE)) {
  install.packages("keyring")
}


library(keyring)

# ---------------------------------------------------------------------------
# 1. Databricks personal access token
#    Used as the password in DatabaseConnector::createConnectionDetails()
#
#    When prompted, paste your Databricks PAT (starts with "dapi...")
# ---------------------------------------------------------------------------
keyring::key_set(
  service  = "databricks",
  username = "token",
  prompt   = "Enter Databricks personal access token: "
)
message("Databricks token stored successfully.")

# ---------------------------------------------------------------------------
# 2. Databricks JDBC connection string
#    Used as the connectionString in DatabaseConnector::createConnectionDetails()
#
#    When prompted, paste the full JDBC connection string, e.g.:
#    jdbc:databricks://adb-XXXX.azuredatabricks.net:443/default;transportMode=http;...
# ---------------------------------------------------------------------------
keyring::key_set(
  service  = "databricks",
  username = "connection_string",
  prompt   = "Enter Databricks JDBC connection string: "
)
message("Databricks connection string stored successfully.")

# ---------------------------------------------------------------------------
# 3. ATLAS WebAPI – Windows credentials
#    ROhdsiWebApi::authorizeWebApi() with authMethod = "windows" uses the
#    current Windows session by default, so no stored secret is needed.
#    If your WebAPI uses Basic auth instead, uncomment the block below
#    and run it to store credentials.
# ---------------------------------------------------------------------------

# keyring::key_set(
#   service  = "atlas_webapi",
#   username = "your_domain\\your_username",   # e.g. "JNJ\\jdoe"
#   prompt   = "Enter ATLAS WebAPI password: "
# )
# message("ATLAS WebAPI password stored successfully.")

# ---------------------------------------------------------------------------
# Verify stored keys (shows service + username, NOT the secret itself)
# ---------------------------------------------------------------------------
message("\nCurrently stored keyring entries:")
print(keyring::key_list())
