const { Kafka, CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");
const { createClient } = require("@clickhouse/client");
const { generateAuthToken } = require("aws-msk-iam-sasl-signer-js");
const { loadSecrets } = require("./secrets");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

// Secret Configuration
const SECRET_NAME = process.env.SECRET_NAME || "in-app-events-credentials";
const AWS_REGION = process.env.AWS_REGION || "us-east-1";

let isConsumerConnected = false;
let batchBuffer = {};
let batchCount = 0;
let consumer;
let clickhouseClient;
let allRoutes = [];
let batchSize;
let insertIntervalMs;

const configManager = {
  async fetchConfig() {
    try {
      const resultSet = await clickhouseClient.query({
        query:
          "SELECT er_kafka_topic, ch_db_name, ch_table_name FROM tbl_event_routes",
        format: "JSONEachRow",
      });
      const rows = await resultSet.json();

      allRoutes = [
        ...rows.map((r) => ({
          topic: r.er_kafka_topic,
          database: r.ch_db_name,
          table: r.ch_table_name,
        })),
        {
          topic: "unregistered-route",
          database: "default",
          table: "unregistered_routes",
        },
        {
          topic: "unlisted-package-id",
          database: "default",
          table: "unlisted_package_ids",
        },
      ];
    } catch (err) {
      console.error("Database config error:", err.message);
    }
  },

  startAutoRefresh() {
    const FETCH_INTERVAL_MS = 300000;
    setInterval(() => {
      configManager
        .fetchConfig()
        .catch((e) => console.error("Config refresh error:", e.message));
    }, FETCH_INTERVAL_MS);
  },
};

async function flushBuffer() {
  if (batchCount === 0) return;

  const currentBuffer = batchBuffer;
  batchBuffer = {};
  batchCount = 0;

  for (const [table, rows] of Object.entries(currentBuffer)) {
    try {
      await clickhouseClient.insert({
        table,
        values: rows,
        format: "JSONEachRow",
      });
    } catch (err) {
      console.error(
        `Flush failed for table ${table}, reverting buffer:`,
        err.message,
      );
      if (!batchBuffer[table]) batchBuffer[table] = [];
      batchBuffer[table].unshift(...rows);
      batchCount += rows.length;
    }
  }
}

async function start() {
  try {
    // 1. Load Secrets from AWS
    console.log(`Fetching secrets from AWS: ${SECRET_NAME}...`);
    const secrets = await loadSecrets(SECRET_NAME, AWS_REGION);

    // 2. ClickHouse Configuration
    clickhouseClient = createClient({
      host: secrets.CLICKHOUSE_HOST,
      username: secrets.CLICKHOUSE_USER,
      password: secrets.CLICKHOUSE_PASSWORD,
      database: secrets.CLICKHOUSE_CONFIG_DATABASE || "default",
    });

    batchSize = parseInt(secrets.CLICKHOUSE_INSERT_BATCH_SIZE || "10000", 10);
    insertIntervalMs = parseInt(
      secrets.CLICKHOUSE_INSERT_INTERVAL_MS || "5000",
      10,
    );

    // 3. Setup Timer for Periodic Flushes
    setInterval(() => {
      flushBuffer().catch((e) =>
        console.error("Scheduled flush error:", e.message),
      );
    }, insertIntervalMs);

    // 4. Initial Config Fetch
    await configManager.fetchConfig();
    configManager.startAutoRefresh();

    // 5. Kafka Setup
    const kafkaBrokers = secrets.KAFKA_BROKERS?.split(",");
    const kafkaClientId = secrets.KAFKA_CLIENT_ID;
    const kafkaGroupId = secrets.KAFKA_GROUP_ID;

    if (!kafkaBrokers || !kafkaClientId || !kafkaGroupId) {
      throw new Error("Kafka configuration missing in AWS Secrets");
    }

    const kafka = new Kafka({
      clientId: kafkaClientId,
      brokers: kafkaBrokers,
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          const { token } = await generateAuthToken({ region: AWS_REGION });
          return { value: token };
        },
      },
    });

    consumer = kafka.consumer({ groupId: kafkaGroupId });
    await consumer.connect();
    isConsumerConnected = true;
    console.log("✅ Kafka Consumer connected");

    const topicsToSubscribe = [...new Set(allRoutes.map((r) => r.topic))];
    await consumer.subscribe({
      topics: topicsToSubscribe,
      fromBeginning: false,
    });
    console.log(`📡 Subscribed to topics: ${topicsToSubscribe.join(", ")}`);

    await consumer.run({
      eachBatchAutoResolve: true,
      eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
        if (!clickhouseClient) {
          console.error("Clickhouse client not initialized");
          return;
        }

        const route = allRoutes.find((r) => r.topic === batch.topic);
        if (!route) return;

        const targetPath = `${route.database}.${route.table}`;
        if (!isRunning() || isStale()) return;

        try {
          const payloads = batch.messages.map((m) =>
            JSON.parse(m.value.toString()),
          );
          if (!batchBuffer[targetPath]) {
            batchBuffer[targetPath] = [];
          }

          batchBuffer[targetPath].push(...payloads);
          batchCount += payloads.length;

          if (batchCount >= batchSize) {
            await flushBuffer();
          }
        } catch (err) {
          console.error(
            `Error processing batch for topic ${batch.topic}:`,
            err.message,
          );
          throw err;
        }
        await heartbeat();
      },
    });
  } catch (err) {
    console.error("Bootstrap failed:", err.message);
    process.exit(1);
  }
}

start();

// Graceful Shutdown
["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, async () => {
    console.log(`🛑 Shutting down (${signal})...`);
    try {
      await flushBuffer();
      if (isConsumerConnected) {
        await consumer.disconnect();
        isConsumerConnected = false;
      }
      process.exit(0);
    } catch (err) {
      console.error("Error during graceful shutdown:", err.message);
      process.exit(1);
    }
  });
});
