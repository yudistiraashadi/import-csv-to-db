import { drizzle } from "drizzle-orm/postgres-js";
import { pgTable, text, timestamp, date, bigint } from "drizzle-orm/pg-core";
import { z } from "zod";
import postgres from "postgres";
import assert from "node:assert";
import fs from "node:fs";
import csv from "csv-parser";
import { sql } from "drizzle-orm";

/**
 * CONFIGURATION
 */
const postgresClientConnection = process.env.POSTGRES_CONNECTION_STRING;
assert(postgresClientConnection, "POSTGRES_CONNECTION_STRING is required");

// CHANGE HERE (ACCORDING TO THE CSV FILE)
const fileValidationScheme = z.object({
  crawl_timestamp: z.string().datetime().optional(),
  url: z.string().url(),
  title: z.string(),
  article: z.string(),
  author: z.string().optional(),
  date: z.string().date(),
});

type IFileRecord = z.infer<typeof fileValidationScheme>;

const platformId = 2;

// CHANGE HERE
const csvFilePath =
  "/home/yudis-dev/Codes/Research/sun-sentiment/web-crawler/output/20240825064539_asianinvestor.net.csv";
/**
 * END OF CONFIGURATION
 */

// Public Schema
export const platforms = pgTable("platforms", {
  id: bigint("id", { mode: "number" }).primaryKey().generatedAlwaysAsIdentity(),
  name: text("name").notNull(),
});

export const articles = pgTable("articles", {
  id: bigint("id", { mode: "number" }).primaryKey().generatedAlwaysAsIdentity(),
  crawlTimestamp: timestamp("crawl_timestamp"),
  articleDate: date("article_date").notNull(),
  url: text("url").notNull().unique(),
  title: text("title").notNull(),
  content: text("content").notNull(),
  platformId: bigint("platform_id", { mode: "number" })
    .notNull()
    .references(() => platforms.id),
  author: text("author"),
});
// End of Public Schema

const queryClient = postgres(postgresClientConnection);
const db = drizzle(queryClient, {
  schema: {
    platforms,
    articles,
  },
});

const csvParser = csv({
  strict: true,
});

const records: IFileRecord[] = [];

// Use the readable stream api to consume records
csvParser.on("data", function (data) {
  try {
    const validatedRecord = fileValidationScheme.parse(data);
    records.push(validatedRecord);
  } catch (error) {
    console.error(error);
    console.error("Error parsing record: ", data);
    console.error("Current length: " + records.length);
    process.exit(1);
  }
});
// Catch any error
csvParser.on("error", function (err) {
  console.error(err);
  process.exit(1);
});

csvParser.on("end", async function () {
  console.log("success parsing csv file");
  console.log("Total records: ", records.length);

  try {
    // Insert articles
    await db.transaction(async (tx) => {
      // create map to remove duplicate records based on URL
      const articlesToInsertMap = new Map();
      records.forEach((record) => {
        // if (articlesToInsertMap.has(record.url)) {
        //   console.log("Duplicate URL: ", record.url);
        // }

        // CHANGE HERE (ACCORDING TO THE CSV FILE)
        articlesToInsertMap.set(record.url, {
          platformId: platformId,
          crawlTimestamp: record.crawl_timestamp
            ? new Date(record.crawl_timestamp)
            : undefined,
          articleDate: record.date,
          url: record.url,
          title: record.title,
          content: record.article,
          author: record.author,
        });
      });

      const articlesToInsert = Array.from(articlesToInsertMap.values());

      // insert 1k records at a time
      for (let i = 0; i < articlesToInsert.length; i += 1000) {
        const articlesToInsertTemp = articlesToInsert.slice(i, i + 1000);

        await tx
          .insert(articles)
          .values(articlesToInsertTemp)
          .onConflictDoUpdate({
            target: articles.url,
            set: {
              crawlTimestamp: sql`EXCLUDED.crawl_timestamp`,
              articleDate: sql`EXCLUDED.article_date`,
              title: sql`EXCLUDED.title`,
              content: sql`EXCLUDED.content`,
              author: sql`EXCLUDED.author`,
            },
          });
      }
    });
  } catch (error) {
    console.error(error);
    process.exit(1);
  }

  console.log(`Articles inserted. Total insert length ${records.length}`);
  process.exit(0);
});

// Read the CSV file
fs.createReadStream(csvFilePath).pipe(csvParser);
