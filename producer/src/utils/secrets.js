const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require("@aws-sdk/client-secrets-manager");

/**
 * Fetches secrets from AWS Secrets Manager and returns them as a JSON object.
 * @param {string} secretName - The name of the secret to fetch.
 * @param {string} region - The AWS region where the secret is stored.
 * @returns {Promise<Object>} - The parsed secret object.
 */
async function loadSecrets(secretName, region = "us-east-1") {
  const client = new SecretsManagerClient({ region });

  try {
    const response = await client.send(
      new GetSecretValueCommand({
        SecretId: secretName,
        VersionStage: "AWSCURRENT",
      }),
    );

    if (!response.SecretString) {
      throw new Error(`Secret ${secretName} has no SecretString`);
    }

    return JSON.parse(response.SecretString);
  } catch (error) {
    console.error(`Failed to load secrets from AWS (${secretName}):`, error);
    throw error;
  }
}

module.exports = { loadSecrets };
