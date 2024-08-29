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
  platform: z.string(),
  url: z.string().url(),
  title: z.string(),
  article: z.string(),
  author: z.string().optional(),
  date: z.string().date(),
});

type IFileRecord = z.infer<typeof fileValidationScheme>;

//  CHANGE HERE (ACCORDING TO THE CSV FILE) [key, value]
const fileToDbMap = new Map([
  ["crawlTimestamp", "crawl_timestamp"],
  ["url", "url"],
  ["title", "title"],
  ["content", "article"],
  ["author", "author"],
  ["articleDate", "date"],
]);

const platformId = 1;

// CHANGE HERE
const csvFilePath =
  "/home/yudis-dev/Codes/Research/sun-sentiment/web-crawler/output/20240817203519_bisnis.com.csv";
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
    await db.transaction(async (tx) => {
      // Insert articles
      const articlesToInsert = records.map((record) => {
        // CHANGE HERE (ACCORDING TO THE CSV FILE)
        return {
          platformId: platformId,
          crawlTimestamp: record.crawl_timestamp
            ? new Date(record.crawl_timestamp)
            : undefined,
          articleDate: record.date,
          url: record.url,
          title: record.title,
          content: record.article,
          author: record.author,
        };
        // // const crawlTimestampKey = fileToDbMap.get("crawlTimestamp");
        // // const articleDateKey = fileToDbMap.get("articleDate");
        // // const urlKey = fileToDbMap.get("url");
        // // const titleKey = fileToDbMap.get("title");
        // // const contentKey = fileToDbMap.get("content");
        // // const authorKey = fileToDbMap.get("author");

        // // if (
        // //   !crawlTimestampKey ||
        // //   !articleDateKey ||
        // //   !urlKey ||
        // //   !titleKey ||
        // //   !contentKey ||
        // //   !authorKey
        // // ) {
        // //   console.error(record);
        // //   console.error({
        // //     crawlTimestampKey,
        // //     articleDateKey,
        // //     urlKey,
        // //     titleKey,
        // //     contentKey,
        // //     authorKey,
        // //   });
        // //   console.error("One of the required keys is missing");
        // //   process.exit(1);
        // // }

        // return {
        //   // @ts-expect-error
        //   crawlTimestamp: new Date(record[crawlTimestampKey]),
        //   // @ts-expect-error
        //   articleDate: record[articleDateKey],
        //   // @ts-expect-error
        //   url: record[urlKey],
        //   // @ts-expect-error
        //   title: record[titleKey],
        //   // @ts-expect-error
        //   content: record[contentKey],
        //   // @ts-expect-error
        //   author: record[authorKey],
        //   platformId: platformId,
        // };
      });

      // await tx.insert(articles).values(articlesToInsert);

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
