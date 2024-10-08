import { drizzle } from "drizzle-orm/postgres-js";
import { pgTable, text, timestamp, date, bigint } from "drizzle-orm/pg-core";
import { z } from "zod";
import postgres from "postgres";
import assert from "node:assert";
import fs from "node:fs";
import csv from "csv-parser";
import { sql } from "drizzle-orm";
import path from "node:path";

/**
 * CONFIGURATION
 */
const postgresClientConnection = process.env.POSTGRES_CONNECTION_STRING;
assert(postgresClientConnection, "POSTGRES_CONNECTION_STRING is required");

// CHANGE HERE (ACCORDING TO THE CSV FILE)
// Original headers = ["crawl_timestamp", "platform", "url", "title", "author", "date", "article"],
// Below we modify the headers to match the database schema
const csvHeaders = [
  "crawlTimestamp",
  "platform",
  "url",
  "title",
  "author",
  "articleDate",
  "content",
];

// CHANGE HERE (ACCORDING TO THE CSV FILE)
const fileValidationScheme = z.object({
  crawlTimestamp: z.string().datetime().optional(),
  url: z.string().url(),
  title: z.string(),
  content: z.string().min(1),
  author: z.string().optional(),
  articleDate: z.string().date(),
});

type IFileRecord = z.infer<typeof fileValidationScheme>;

const platformId = 7;

//Hard coded directory has been used.
// CHANGE HERE (FOR MULTIPLE CSV FILES)
// MAKE THIS EMPTY IF YOU WANT TO USE SINGLE CSV FILE
const csvDirPath = "";
//   "/home/yudis/Codes/Research/ITS/SUN-sentiment/import-csv-to-db/data/bisnis-crawler/scraped_articles/";

// CHANGE HERE (FOR SINGLE CSV FILE)
// MAKE THIS EMPTY IF YOU WANT TO USE MULTIPLE CSV FILES
const csvFilePath =
  "/home/yudis-dev/Codes/Research/sun-sentiment/web-crawler/output/20240829174851_businessinsider.com.csv";
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

// Read the CSV file
if ((csvDirPath && csvFilePath) || (!csvDirPath && !csvFilePath)) {
  console.error("Please specify either csvDirPath or csvFilePath");
  process.exit(1);
}

let dirResult: string[];

if (csvDirPath) {
  dirResult = fs.readdirSync(csvDirPath);
} else {
  dirResult = [csvFilePath];
}

dirResult.forEach((file) => {
  let csvFilePath: string;

  if (csvDirPath) {
    csvFilePath = path.join(csvDirPath, file);
  } else {
    csvFilePath = file;
  }

  const records: IFileRecord[] = [];

  fs.createReadStream(csvFilePath)
    .pipe(
      csv({
        strict: true,
        headers: csvHeaders,
        skipLines: 1,
      })
    )
    .on("data", function (data) {
      try {
        const validatedRecord = fileValidationScheme.parse(data);
        records.push(validatedRecord);
      } catch (error) {
        console.error(error);
        console.error("Error parsing record: ", data);
        console.error("Current length: " + records.length);
        process.exit(1);
      }
    })
    .on("error", function (err) {
      console.error(err);
      process.exit(1);
    })
    .on("end", async function () {
      console.log(`success parsing csv file: ${csvFilePath}`);
      console.log("Total records: ", records.length);

      try {
        // Insert articles
        await db.transaction(async (tx) => {
          // create map to remove duplicate records based on URL
          const articlesToInsertMap = new Map();
          records.forEach((record) => {
            articlesToInsertMap.set(record.url, {
              platformId: platformId,
              crawlTimestamp: record.crawlTimestamp
                ? new Date(record.crawlTimestamp)
                : undefined,
              articleDate: record.articleDate,
              title: record.title,
              content: record.content,
              author: record.author,
              url: record.url,
            });
          });

          const articlesToInsert = Array.from(articlesToInsertMap.values());

          if (articlesToInsert.length === 0) {
            console.log("No new articles to insert");
            return;
          }

          // insert 1k records at a time
          for (let i = 0; i < articlesToInsert.length; i += 1000) {
            const articlesToInsertTemp = articlesToInsert.slice(i, i + 1000);

            if (articlesToInsertTemp.length === 0) {
              break;
            }

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

          console.log(`Inserted articles length: ${articlesToInsert.length}`);
        });
      } catch (error) {
        console.error(error);
        process.exit(1);
      }
    });
});
