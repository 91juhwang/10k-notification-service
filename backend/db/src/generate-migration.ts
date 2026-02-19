import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const name = process.argv[2];
if (!name) {
  console.error("Usage: pnpm run db:generate -- <migration-name>");
  process.exit(1);
}

const now = new Date();
const stamp = [
  now.getUTCFullYear(),
  String(now.getUTCMonth() + 1).padStart(2, "0"),
  String(now.getUTCDate()).padStart(2, "0"),
  String(now.getUTCHours()).padStart(2, "0"),
  String(now.getUTCMinutes()).padStart(2, "0"),
  String(now.getUTCSeconds()).padStart(2, "0")
].join("");

const filename = `${stamp}_${name.replace(/[^a-z0-9_-]/gi, "_").toLowerCase()}.sql`;
const migrationsDir = path.resolve(__dirname, "../migrations");

await mkdir(migrationsDir, { recursive: true });
await writeFile(path.join(migrationsDir, filename), "-- Write migration SQL here\n", "utf8");

console.log(`[db] created migration backend/db/migrations/${filename}`);
