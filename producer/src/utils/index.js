const crypto = require("crypto");
const { CompressionTypes } = require("kafkajs");

const decryptAlgorithm = "aes-256-cbc";

const BATCH_SIZE = 1000;
const LINGER_MS = 100;
const buffers = new Map();
const timers = new Map();

function decrypt(encryptedData, packageSecret) {
  try {
    const key = Buffer.from(packageSecret, "hex");
    const ivBuffer = Buffer.from(packageSecret.substring(0, 32), "hex");
    const decipher = crypto.createDecipheriv(decryptAlgorithm, key, ivBuffer);
    let decrypted = decipher.update(encryptedData, "hex", "utf8");
    decrypted += decipher.final("utf8");
    return { payloadJson: JSON.parse(decrypted) };
  } catch (error) {
    return { error: "Something went wrong", status: 400 };
  }
}

function encrypt(data, packageSecret) {
  try {
    const key = Buffer.from(packageSecret, "hex");
    const ivBuffer = Buffer.from(packageSecret.substring(0, 32), "hex");
    const cipher = crypto.createCipheriv(decryptAlgorithm, key, ivBuffer);
    let encrypted = cipher.update(data, "utf8", "hex");
    encrypted += cipher.final("hex");
    return encrypted;
  } catch (error) {
    return null;
  }
}

function requiredFieldValidation(payload, requiredFields) {
  for (let i = 0; i < requiredFields.length; i++) {
    const value = payload[requiredFields[i]];
    if (value == null) return { isValid: false };
    if (typeof value === "string" && value.trim() === "") {
      return { isValid: false };
    }
  }
  return { isValid: true };
}

function flushMessages(topic, producer) {
  const batch = buffers.get(topic);
  if (!batch || batch.length === 0) return;

  buffers.set(topic, []);

  producer
    .send({
      topic,
      acks: -1,
      messages: batch,
      compression: CompressionTypes.Snappy,
    })
    .catch((error) => {
      const currentBuffer = buffers.get(topic) || [];
      buffers.set(topic, [...batch, ...currentBuffer]);
      console.error("Kafka flush failed. Restoring messages.", {
        err: error.message,
        topic,
        size: batch.length,
      });
    });
}

function enqueueEvent(topic, payload, producer) {
  if (!buffers.has(topic)) buffers.set(topic, []);
  const buffer = buffers.get(topic);

  buffer.push({ value: JSON.stringify(payload) });

  if (buffer.length >= BATCH_SIZE) {
    if (timers.has(topic)) {
      clearTimeout(timers.get(topic));
      timers.delete(topic);
    }
    flushMessages(topic, producer);
  } else if (!timers.has(topic)) {
    timers.set(
      topic,
      setTimeout(() => {
        flushMessages(topic, producer);
        timers.delete(topic);
      }, LINGER_MS),
    );
  }
}

module.exports = {
  decrypt,
  encrypt,
  requiredFieldValidation,
  enqueueEvent,
};
