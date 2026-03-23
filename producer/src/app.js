process.env.TZ = "UTC";
const fastify = require("fastify")({
  logger: {
    level: "info",
  },
});

const {
  Kafka,
  CompressionTypes,
  CompressionCodecs,
  logLevel,
} = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");
const { createClient } = require("@clickhouse/client");
const { generateAuthToken } = require("aws-msk-iam-sasl-signer-js");
const { loadSecrets } = require("./utils/secrets");
const eventRoutes = require("./routes/eventRoutes");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// Secret Configuration
const SECRET_NAME = "in-app-events-credentials";
const AWS_REGION = "us-east-1";

// Global Clients (Initialized in app())
let clickhouseClient;
let kafka;
let producer;
let configManager;

const app = async () => {
  const port = 3004;
  try {
    // 1. Load Secrets from AWS
    fastify.log.info(`Fetching secrets from AWS: ${SECRET_NAME}...`);
    const secrets = await loadSecrets(SECRET_NAME, AWS_REGION);

    // 2. Initialize ClickHouse Client
    clickhouseClient = createClient({
      host: secrets.CLICKHOUSE_HOST,
      username: secrets.CLICKHOUSE_USER,
      password: secrets.CLICKHOUSE_PASSWORD,
      database: secrets.CLICKHOUSE_CONFIG_DATABASE || "default",
    });

    // 3. Config Manager Setup
    const fetchIntervalMs = parseInt(secrets.CONFIG_REFRESH_MS || "300000", 10);
    let configCache = { packageSecrets: {}, routeMap: {}, routes: [] };

    configManager = {
      async fetchConfig() {
        try {
          const secretsResultSet = await clickhouseClient.query({
            query: "SELECT app_package_id, app_package_secret FROM tbl_apps",
            format: "JSONEachRow",
          });
          const rawSecrets = await secretsResultSet.json();
          const newSecrets = rawSecrets.reduce((acc, row) => {
            acc[row.app_package_id] = row.app_package_secret;
            return acc;
          }, {});

          const routesResultSet = await clickhouseClient.query({
            query:
              "SELECT er_api_path, er_kafka_topic, ch_db_name, ch_table_name, er_required_fields FROM tbl_event_routes",
            format: "JSONEachRow",
          });
          const newRoutes = await routesResultSet.json();

          const newRouteMap = newRoutes.reduce((acc, raw) => {
            acc[raw.er_api_path] = {
              topic: raw.er_kafka_topic,
              database: raw.ch_db_name,
              table: raw.ch_table_name,
              requiredFields: raw.er_required_fields || [],
            };
            return acc;
          }, {});

          configCache = {
            packageSecrets: newSecrets,
            routeMap: newRouteMap,
            routes: newRoutes,
          };
          fastify.log.info("Configuration refreshed from ClickHouse");
        } catch (err) {
          fastify.log.error(`Failed to fetch configuration: ${err.message}`);
        }
      },

      startAutoRefresh() {
        setInterval(() => {
          configManager
            .fetchConfig()
            .catch((e) =>
              fastify.log.error(`Config refresh error: ${e.message}`),
            );
        }, fetchIntervalMs);
      },

      getAppSecret: (packageName) => configCache.packageSecrets[packageName],
      getRoute: (routeName) => configCache.routeMap[routeName],
    };

    // 5. Kafka Setup
    const kafkaBrokers = secrets.KAFKA_BROKERS?.split(",");
    const clientId = secrets.KAFKA_CLIENT_ID;

    if (!kafkaBrokers || !clientId) {
      throw new Error("Kafka configuration missing in AWS Secrets");
    }

    kafka = new Kafka({
      clientId,
      brokers: kafkaBrokers,
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          const { token } = await generateAuthToken({ region: AWS_REGION });
          return { value: token };
        },
      },
      logLevel: logLevel.ERROR,
    });

    producer = kafka.producer({
      allowAutoTopicCreation: false,
      idempotent: true,
      maxInFlightRequests: 5,
      retry: { retries: 5 },
    });

    // 6. Fastify Decoration
    fastify.decorate("configManager", configManager);
    fastify.decorate("producer", producer);

    // 7. Middlewares & Hooks
    fastify.addContentTypeParser(
      "text/plain",
      { parseAs: "string" },
      (req, body, done) => done(null, body),
    );
    fastify.addContentTypeParser(
      "application/octet-stream",
      { parseAs: "string" },
      (req, body, done) => done(null, body),
    );

    fastify.addHook("onSend", async (request, reply, payload) => {
      if (request.packageSecret && payload) {
        try {
          const dataToEncrypt =
            typeof payload === "string" ? payload : JSON.stringify(payload);
          const encryptedBody = encrypt(dataToEncrypt, request.packageSecret);
          if (encryptedBody) {
            reply.header("Content-Type", "text/plain");
            return encryptedBody;
          }
        } catch (e) {
          fastify.log.error("Encryption hook failed", e);
        }
      }
      return payload;
    });

    // 8. Finalize Startup
    fastify.register(eventRoutes);
    await configManager.fetchConfig();
    configManager.startAutoRefresh();
    await producer.connect();
    fastify.log.info("Kafka producer connected");

    await fastify.listen({ port, host: "0.0.0.0" });
    fastify.log.info(`Producer server listening on ${port}`);

    return fastify;
  } catch (err) {
    fastify.log.error(`App startup failed: ${err.message}`);
    process.exit(1);
  }
};

// Graceful Shutdown
["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, async () => {
    fastify.log.info(`Shutting down (${signal})...`);
    try {
      await producer?.disconnect();
      await fastify.close();
      process.exit(0);
    } catch (error) {
      fastify.log.error(`Error during shutdown: ${error.message}`);
      process.exit(1);
    }
  });
});

app();
