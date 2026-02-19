const pollWait = Number(process.env.WORKER_POLL_WAIT_SECONDS ?? 20);

console.log("[worker] phase 0 bootstrap started");
console.log(`[worker] configured poll wait seconds: ${pollWait}`);
console.log("[worker] phase 1 will attach SQS consumers and processors");

setInterval(() => {
  console.log("[worker] heartbeat");
}, 30000);
