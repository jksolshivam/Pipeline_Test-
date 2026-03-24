const { Kafka, CompressionTypes, CompressionCodecs } = require("kafkajs");
const SnappyCodec = require("kafkajs-snappy");
const { createClient } = require("@clickhouse/client");
const { generateAuthToken } = require("aws-msk-iam-sasl-signer-js");
const { loadSecrets } = require("./secrets");

CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec;

const SECRET_NAME = "in-app-events-credentials";
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const CONFIG_REFRESH_MS = 300000;

let consumer;
let kafka;
let clickhouseClient;

let isConsumerConnected = false;
let isShuttingDown = false;
let batchBuffer = {};
let batchCount = 0;
let allRoutes = [];
let flushIntervalHandle;
let flushInFlight = null;

let batchSize;
let insertIntervalMs;
let kafkaGroupId;

function getPartitionKey(topic, partition) {
  return `${topic}:${partition}`;
}

function getNextOffset(offset) {
  return (BigInt(offset) + 1n).toString();
}

function mergeOffsets(target, source) {
  for (const [partitionKey, offsetData] of Object.entries(source || {})) {
    const current = target[partitionKey];
    if (!current || BigInt(offsetData.offset) > BigInt(current.offset)) {
      target[partitionKey] = offsetData;
    }
  }
}

function ensureTableBuffer(table) {
  if (!batchBuffer[table]) {
    batchBuffer[table] = {
      rows: [],
      offsets: {},
    };
  }

  return batchBuffer[table];
}

function restoreTableBuffer(table, tableBuffer) {
  const current = ensureTableBuffer(table);
  current.rows.unshift(...tableBuffer.rows);
  mergeOffsets(current.offsets, tableBuffer.offsets);
  batchCount += tableBuffer.rows.length;
}

function buildCommitOffsets(offsetMap) {
  return Object.values(offsetMap).map(({ topic, partition, offset }) => ({
    topic,
    partition,
    offset,
  }));
}

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

      allRoutes = [
        ...rows.map((route) => ({
          topic: route.er_kafka_topic,
          database: route.ch_db_name,
          table: route.ch_table_name,
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

      console.log(`Loaded ${allRoutes.length} route mappings`);
    } catch (err) {
      console.error("Database config error:", err.message);
    }
  },

  startAutoRefresh() {
    setInterval(() => {
      configManager
        .fetchConfig()
        .catch((err) => console.error("Config refresh error:", err.message));
    }, CONFIG_REFRESH_MS);
  },
};

// -------------------- BUFFER --------------------

async function flushBufferInternal() {
  if (batchCount === 0) {
    return;
  }

  const currentBuffer = batchBuffer;
  const totalRows = batchCount;
  const committedOffsets = {};

  batchBuffer = {};
  batchCount = 0;

  console.log(`Flushing ${totalRows} buffered rows to ClickHouse`);

  for (const [table, tableBuffer] of Object.entries(currentBuffer)) {
    try {
      await clickhouseClient.insert({
        table,
        values: tableBuffer.rows,
        format: "JSONEachRow",
      });

      mergeOffsets(committedOffsets, tableBuffer.offsets);
      console.log(`Flushed ${tableBuffer.rows.length} rows to ${table}`);
    } catch (err) {
      console.error(`Flush failed for ${table}:`, err.message);
      restoreTableBuffer(table, tableBuffer);
    }
  }

  const offsetsToCommit = buildCommitOffsets(committedOffsets);
  if (offsetsToCommit.length > 0) {
    await consumer.commitOffsets(offsetsToCommit);
    console.log(`Committed ${offsetsToCommit.length} Kafka partition offsets`);
  }
}

async function flushBuffer() {
  if (flushInFlight) {
    return flushInFlight;
  }

  flushInFlight = (async () => {
    try {
      await flushBufferInternal();
    } finally {
      flushInFlight = null;
    }
  })();

  return flushInFlight;
}

// -------------------- CONSUMER --------------------

async function runConsumer() {
  await consumer.subscribe({
    topic: /^event-.*$/,
    fromBeginning: false,
  });

  await consumer.run({
    eachBatchAutoResolve: false,
    autoCommit: false,

    eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
      if (!clickhouseClient) {
        console.error("ClickHouse client is not initialized");
        return;
      }

      if (!isRunning() || isStale() || isShuttingDown) {
        return;
      }

      const route = allRoutes.find((item) => item.topic === batch.topic);
      if (!route) {
        console.warn(`No route found for topic ${batch.topic}, skipping batch`);
        return;
      }

      const targetPath = `${route.database}.${route.table}`;
      const tableBuffer = ensureTableBuffer(targetPath);

      try {
        console.log(
          `Received batch topic=${batch.topic} partition=${batch.partition} messages=${batch.messages.length}`,
        );

        for (const message of batch.messages) {
          const parsedValue = JSON.parse(message.value.toString());
          tableBuffer.rows.push(parsedValue);
          batchCount += 1;

          const partitionKey = getPartitionKey(batch.topic, batch.partition);
          tableBuffer.offsets[partitionKey] = {
            topic: batch.topic,
            partition: batch.partition,
            offset: getNextOffset(message.offset),
          };

          await heartbeat();
        }

        console.log(
          `Buffered ${batch.messages.length} rows for ${targetPath}. Total buffered rows: ${batchCount}`,
        );

        if (batchCount >= batchSize) {
          await flushBuffer();
        }
      } catch (err) {
        console.error(`Error processing ${batch.topic}:`, err.message);
        throw err;
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

    batchSize = parseInt(secrets.CLICKHOUSE_INSERT_BATCH_SIZE || "10000", 10);
    insertIntervalMs = parseInt(
      secrets.CLICKHOUSE_INSERT_INTERVAL_MS || "5000",
      10,
    );

    flushIntervalHandle = setInterval(() => {
      flushBuffer().catch((err) =>
        console.error("Scheduled flush failed:", err.message),
      );
    }, insertIntervalMs);

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

    consumer = kafka.consumer({
      groupId: kafkaGroupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });

    await consumer.connect();
    isConsumerConnected = true;

    console.log("Kafka consumer connected");

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
    if (isShuttingDown) {
      return;
    }

    isShuttingDown = true;
    console.log(`Shutdown requested (${signal})`);

    if (flushIntervalHandle) {
      clearInterval(flushIntervalHandle);
    }

    try {
      await flushBuffer();
    } catch (err) {
      console.error("Final flush failed during shutdown:", err.message);
    }

    if (isConsumerConnected) {
      await consumer.disconnect();
      isConsumerConnected = false;
    }

    process.exit(0);
  });
});
