const { decrypt } = require("../utils");

async function isRequestValid(request, reply) {
  const requestTimeHeader = request.headers["x-ts"];
  const packageName = request.headers["x-package-id"];

  if (!packageName) {
    return reply.status(400).send({ error: "Missing x-package-id header" });
  }

  if (!requestTimeHeader) {
    return reply.status(400).send({ error: "Missing x-ts header" });
  }

  const diffTime = Date.now() - Date.parse(requestTimeHeader);
  const timeOut = 300000; // 5 minute timeout

  if (diffTime > timeOut) {
    return reply.status(400).send({ error: "Request timed out" });
  }

  // Access configManager via request.server (Fastify instance)
  const packageSecret = request.server.configManager.getAppSecret(packageName);

  if (packageSecret) {
    request.rawBody = request.body; // Store raw body for logging/DLQ

    const decryptResult = decrypt(request.rawBody, packageSecret);
    if (decryptResult.error) {
      return reply
        .status(decryptResult.status)
        .send({ error: decryptResult.error });
    }

    request.payloadJson = decryptResult.payloadJson;
  }

  request.packageSecret = packageSecret;
}

module.exports = isRequestValid;
