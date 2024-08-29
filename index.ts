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
const fileValidationScheme = z.object({
  scrapping_date: z.string().datetime().optional(),
  link: z.string().url(),
  title: z.string(),
  content: z.string().min(1),
  author: z.string().optional(),
  article_date: z.string().date(),
});

type IFileRecord = z.infer<typeof fileValidationScheme>;

const platformId = 1;

//Hard coded directory has been used.
// CHANGE HERE (FOR MULTIPLE CSV FILES)
const csvDirPath =
  "/home/yudis/Codes/Research/ITS/SUN-sentiment/import-csv-to-db/data/bisnis-crawler/scraped_articles/";

// CHANGE HERE (FOR SINGLE CSV FILE)
// const csvFilePath =
//   "/home/yudis/Codes/Research/ITS/SUN-sentiment/import-csv-to-db/data/bisnis-crawler/scraped_articles/Analisis_Sentimen_scraped_articles.csv";
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
fs.readdirSync(csvDirPath)
  .filter((filename: string) => {
    return filename.split(".")[1] == "csv";
  })
  .forEach((file) => {
    const csvFilePath = path.join(csvDirPath, file);

    const records: IFileRecord[] = [];

    fs.createReadStream(csvFilePath)
      .pipe(
        csv({
          strict: true,
          headers: [
            "title",
            "scrapping_date",
            "article_date",
            "author",
            "link",
            "content",
          ],
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
              articlesToInsertMap.set(record.link, {
                platformId: platformId,
                crawlTimestamp: record.scrapping_date
                  ? new Date(record.scrapping_date)
                  : undefined,
                articleDate: record.article_date,
                title: record.title,
                content: record.content,
                author: record.author,
                url: record.link,
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
