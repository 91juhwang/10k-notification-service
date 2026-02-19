const target = process.env.API_BASE_URL ?? "http://localhost:4000";
const endpoint = `${target}/api/v1/telemetry`;

console.log("[simulator] phase 0 placeholder");
console.log(`[simulator] phase 2 will start sending telemetry to ${endpoint}`);
