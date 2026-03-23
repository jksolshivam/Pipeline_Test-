const http = require("http");
const crypto = require("crypto");

// 1. Config (Matches setup.sql)
const SECRET = "7061636b6167655f7365637265745f3132333435363738393031323334353637";
const PACKAGE_ID = "com.example.myapp";
const ROUTE_PATH = "v1/events"; // Hits /event/v1/events

// 2. Encryption logic (Matches producer/src/utils/index.js)
const key = Buffer.from(SECRET, "hex");
const iv = Buffer.from(SECRET.substring(0, 32), "hex");

function encrypt(data) {
  const cipher = crypto.createCipheriv("aes-256-cbc", key, iv);
  let encrypted = cipher.update(JSON.stringify(data), "utf8", "hex");
  encrypted += cipher.final("hex");
  return encrypted;
}

// 3. Payload
const payload = {
  event_name: "test_event",
  device_id: "device_001",
  other_data: "This is a test event from the script!"
};

const encryptedBody = encrypt(payload);

// 4. Request
const options = {
  hostname: "localhost",
  port: 3004,
  path: `/event/${ROUTE_PATH}`,
  method: "POST",
  headers: {
    "Content-Type": "text/plain",
    "x-package-id": PACKAGE_ID,
    "x-ts": new Date().toISOString(),
    "Content-Length": Buffer.byteLength(encryptedBody)
  }
};

const req = http.request(options, (res) => {
  let data = "";
  res.on("data", (chunk) => { data += chunk; });
  res.on("end", () => {
    console.log(`Status Code: ${res.statusCode}`);
    console.log(`Response: ${data}`);
  });
});

req.on("error", (error) => {
  console.error(`Error: ${error.message}`);
});

req.write(encryptedBody);
req.end();

console.log(`Sending encrypted event to ${options.hostname}:${options.port}${options.path}...`);
