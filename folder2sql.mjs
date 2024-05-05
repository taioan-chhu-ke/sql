import * as Path from "node:path";
import { readdir, open, mkdir } from "node:fs/promises";
import { createGzip } from "node:zlib";
import { createWriteStream } from "node:fs";
import { isMainThread, threadId } from "node:worker_threads";
import { pipeline } from "node:stream/promises";
import { v4 as uuidv4 } from "uuid";
import sqlLint from "sql-lint";
import { decode } from "html-entities";
import Piscina from "piscina";

const SEGMENT_LENGTH = Math.pow(10, 3);

async function generateKindList(dirName, kind, kindNameList) {
  const kindSQLName = `${kind}.sql.gz`;
  const gzip = createGzip();
  const writeTo = createWriteStream(Path.resolve(dirName, kindSQLName), {
    autoClose: false,
    flush: true,
  });
  console.log(`[${threadId}] Opening: ${kindSQLName}`);
  await pipeline(async function* () {
    let joinedHeaders;
    let rowList = [];
    async function flush() {
      const sql = `INSERT INTO ${kind}
  (uuid,${joinedHeaders})
VALUES
  (${rowList.join("),\n  (")});\n`;
      rowList.length = 0;
      const errors = await sqlLint.default({
        sql,
      });
      if (errors.length) {
        console.error(errors);
        setTimeout(() => {
          throw new Error(sql);
        });
      }
      return sql;
    }
    for await (const kindName of kindNameList) {
      let filehandle;
      try {
        filehandle = await open(Path.resolve("..", "data-warehouse", dirName, kindName));

        let skipCount = 0;
        for await (const line of filehandle.readLines("utf-8")) {
          if (!joinedHeaders) {
            joinedHeaders = line.trim();
            skipCount += 1;
            continue;
          }
          if (2 > skipCount) {
            skipCount += 1;
            continue;
          }
          if (SEGMENT_LENGTH === rowList.length) {
            yield await flush();
          }
          let cleanedLine = line.trim();
          // cleanedLine = decode(cleanedLine);
          cleanedLine = cleanedLine.replaceAll(/�/g, "");
          cleanedLine = cleanedLine.replaceAll(
            /&#(\d+);/g,
            (src, what) =>
              ({
                128: "",
                65533: "",
                151: "—",
                223: "ß",
                12316: "～",
                24304: "廰",
                27114: "概",
                37921: "鐵",
              })[what] || src
          );
          cleanedLine = cleanedLine.replaceAll(/\s*;\s*/g, "；");
          cleanedLine = cleanedLine.replaceAll("(", "（");
          cleanedLine = cleanedLine.replaceAll(")", "）");

          rowList.push([uuidv4(), cleanedLine].join(","));
        }
      } finally {
        await filehandle?.close();
      }
    }
    yield await flush();
  }, gzip, writeTo);
  writeTo.end();
  console.log(`[${threadId}] Written: ${kindSQLName}`);
}

export default async function generateDir(dirName) {
  const [csvNameList] = await Promise.all([
    readdir(Path.resolve("..", "data-warehouse", dirName)),
    mkdir(Path.resolve(dirName), { recursive: true })
  ]);
  const buildNameList = [];
  const integratedNameList = [];
  const landNameList = [];
  const parkNameList = [];
  const restNameList = [];
  for (let index = 0; index < csvNameList.length; index++) {
    const csvName = csvNameList[index];
    if (!csvName.match("_lvr_land_")) {
      restNameList.push(csvName);
      continue;
    }
    switch (csvName.slice(-6, -1)) {
      case "ld.cs": {
        buildNameList.push(csvName);
        break;
      }
      case "nd.cs": {
        landNameList.push(csvName);
        break;
      }
      case "rk.cs": {
        parkNameList.push(csvName);
        break;
      }
      default: {
        integratedNameList.push(csvName);
        break;
      }
    }
  }
  buildNameList.sort();
  integratedNameList.sort();
  landNameList.sort();
  parkNameList.sort();
  await generateKindList(dirName, "build", buildNameList);
  await generateKindList(dirName, "integrated", integratedNameList);
  await generateKindList(dirName, "land", landNameList);
  await generateKindList(dirName, "park", parkNameList);
}

if (isMainThread) {
  main(process.argv.slice(2)).catch(async (e) => {
    console.error(e);
    process.exit(1);
  });
}

async function main(dirNameList) {
  const piscina = new Piscina({
    filename: import.meta.url,
  });
  console.log("Batch with: ", dirNameList);
  await Promise.all(dirNameList.map((dirName) => piscina.run(dirName)));
}
