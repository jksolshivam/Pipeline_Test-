const { Kafka, CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");
const { createClient } = require("@clickhouse/client");
const { generateAuthToken } = require("aws-msk-iam-sasl-signer-js");
const { loadSecrets } = require("./secrets");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const SECRET_NAME = "in-app-events-credentials";
const AWS_REGION = process.env.AWS_REGION || "us-east-1";

let consumer;
let kafka;
let clickhouseClient;
let healthServer;

let isConsumerConnected = false;
let batchBuffer = {};
let batchCount = 0;
let allRoutes = [];

let batchSize;
let insertIntervalMs;
let kafkaGroupId;

// -------------------- CONFIG MANAGER --------------------

const configManager = {
  async fetchConfig() {
    try {
      const resultSet = await clickhouseClient.query({
        query:
          "SELECT er_kafka_topic, ch_db_name, ch_table_name FROM tbl_event_routes",
        format: "JSONEachRow",
      });

      const rows = await resultSet.json();

      const newRoutes = [
        ...rows.map((r) => ({
          topic: r.er_kafka_topic,
          database: r.ch_db_name,
          table: r.ch_table_name,
        })),
        {
          topic: "event-unregistered-route",
          database: "default",
          table: "unregistered_routes",
        },
        {
          topic: "event-unlisted-package-id",
          database: "default",
          table: "unlisted_package_ids",
        },
      ];

      allRoutes = newRoutes;
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

// -------------------- BUFFER --------------------

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
      console.log(`Flushed ${rows.length} rows to ${table}`);
    } catch (err) {
      console.error(`Flush failed for ${table}:`, err.message);

      if (!batchBuffer[table]) batchBuffer[table] = [];
      batchBuffer[table].unshift(...rows);
      batchCount += rows.length;
    }
  }
}

// -------------------- CONSUMER --------------------

async function runConsumer() {
   await consumer.subscribe({
    topic: /^event-.*$/,
      fromBeginning: false,
    });



await consumer.run({
  eachBatchAutoResolve: false,

  eachBatch: async ({
    batch,
    resolveOffset,
    heartbeat,
    commitOffsetsIfNecessary,
    isRunning,
    isStale,
  }) => {
    if (!isRunning() || isStale()) return;

    const route = allRoutes.find((r) => r.topic === batch.topic);
    if (!route) return;

    const targetPath = `${route.database}.${route.table}`;

    try {
      const payloads = [];

      for (const message of batch.messages) {
        payloads.push(JSON.parse(message.value.toString()));

        // 👇 mark message processed (but NOT committed yet)
        resolveOffset(message.offset);

        await heartbeat();
      }

      if (!batchBuffer[targetPath]) {
        batchBuffer[targetPath] = [];
      }

      batchBuffer[targetPath].push(...payloads);
      batchCount += payloads.length;

      // 🔥 IMPORTANT: flush immediately OR before commit
      if (batchCount >= batchSize) {
        await flushBuffer();
      }

      // ✅ COMMIT ONLY AFTER SUCCESS
      await commitOffsetsIfNecessary();

    } catch (err) {
      console.error(`Error processing ${batch.topic}:`, err.message);

      // ❌ DO NOT COMMIT → Kafka will retry
    }
  },
});
}


// -------------------- START --------------------

async function start() {
  try {
    console.log(`Fetching secrets: ${SECRET_NAME}`);
    const secrets = await loadSecrets(SECRET_NAME, AWS_REGION);

    clickhouseClient = createClient({
      host: secrets.CLICKHOUSE_HOST,
      username: secrets.CLICKHOUSE_USER,
      password: secrets.CLICKHOUSE_PASSWORD,
      database: secrets.CLICKHOUSE_CONFIG_DATABASE || "default",
    });

    batchSize = parseInt(secrets.CLICKHOUSE_INSERT_BATCH_SIZE || "10000");
    insertIntervalMs = parseInt(
      secrets.CLICKHOUSE_INSERT_INTERVAL_MS || "5000",
    );

    setInterval(flushBuffer, insertIntervalMs);

    await configManager.fetchConfig();
    configManager.startAutoRefresh();

    kafkaGroupId = secrets.KAFKA_GROUP_ID;

    kafka = new Kafka({
      clientId: secrets.KAFKA_CLIENT_ID,
      brokers: secrets.KAFKA_BROKERS.split(","),
      ssl: true,
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => {
          const { token } = await generateAuthToken({
            region: AWS_REGION,
          });
          return { value: token };
        },
      },
    });

    consumer = kafka.consumer({ groupId: kafkaGroupId,
      sessionTimeout: 30000,      // 🔥 increase this (default ~10s)  
      heartbeatInterval: 3000, 
     });

    await consumer.connect();
    isConsumerConnected = true;

    console.log("✅ Kafka connected");

    await runConsumer();
  } catch (err) {
    console.error("Startup failed:", err.message);
    process.exit(1);
  }
}

start();

// -------------------- SHUTDOWN --------------------

["SIGINT", "SIGTERM"].forEach((signal) => {
  process.on(signal, async () => {
    console.log(`🛑 Shutdown (${signal})`);

    await flushBuffer();

    if (healthServer) {
      await new Promise((resolve, reject) => {
        healthServer.close((err) => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
    }

    if (isConsumerConnected) {
      await consumer.disconnect();
      isConsumerConnected = false;
    }

    process.exit(0);
  });
});
