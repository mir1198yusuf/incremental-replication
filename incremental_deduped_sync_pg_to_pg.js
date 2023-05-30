require("dotenv").config();
const fs = require("fs");
const cp = require("child_process");
const path = require("path");
const tablesConfig = require("./tables-config.json");
const axios = require("axios");

const srcKnex = require("knex")({
  client: "pg",
  connection: {
    host: process.env.SOURCE_HOST,
    database: process.env.SOURCE_DB,
    user: process.env.SOURCE_USERNAME,
    password: process.env.SOURCE_PASSWORD,
  },
  searchPath: process.env.SOURCE_SCHEMA,
});

const destKnex = require("knex")({
  client: "pg",
  connection: {
    host: process.env.DEST_HOST,
    database: process.env.DEST_DB,
    user: process.env.DEST_USERNAME,
    password: process.env.DEST_PASSWORD,
  },
  searchPath: process.env.DEST_SCHEMA,
});

async function main() {
  log(`Script starting. found ${tablesConfig.length} tables to sync`);

  try {
    for (let i = 0; i < tablesConfig.length; i++) {
      log(`Starting replication for ${tablesConfig[i].name}`);
      await initReplicationForTable(tablesConfig[i]);
      log(`Replication done for ${tablesConfig[i].name}`);
      log(`Remaining tables to sync : ${tablesConfig.length - (i + 1)}`);
    }

    await webhookNotify(`Pipeline ${process.env.PIPELINE_NAME} succeeded`);
    log(`Script completed`);
  } catch (error) {
    log(`Main function error ${error.stack}`, true);
    await webhookNotify(`Pipeline ${process.env.PIPELINE_NAME} failed`);
  } finally {
    process.exit(0);
  }
}

const webhookNotify = async (message) => {
  try {
    log(`Notifying webhook`);
    await axios.post(process.env.WEBHOOK_URL, {
      message: message,
    });
  } catch (error) {
    log(`WebhookNotify function error ${error.stack}`, true);
  }
};

const log = (msg, isError = false) => {
  console.log(
    `${new Date().toISOString()} [${isError ? "ERROR" : "INFO"}] ${msg}...`
  );
};

const initReplicationForTable = async (table) => {
  try {
    const tableName = table.name;
    const replicationKeyField = table.replication_key;
    const uniqueCols = table.unique_cols;
    let isResetNeededForTable = !(await areSchemasSameForTable(tableName));
    if (isResetNeededForTable) {
      await resetDestinationSchemaForTable(tableName);
      await doPgDumpRestoreForTable(tableName);
      await setReplicationKeyStateAfterInitialSync(
        tableName,
        replicationKeyField
      );
    } else {
      const replicationKeyState = loadReplicationKeyStateFromFile(tableName);
      const newReplicationKeyState =
        await performIncrementalReplicationForTable(
          tableName,
          replicationKeyField,
          replicationKeyState
        );
      await deduplicateRowsForTableInDestination(uniqueCols, tableName);
      if (newReplicationKeyState) {
        saveReplicationKeyStateToFile(newReplicationKeyState, tableName);
      }
    }
  } catch (error) {
    log(`InitReplicationForTable function error ${error.stack}`, true);
    throw error;
  }
};

async function setReplicationKeyStateAfterInitialSync(
  tableName,
  replicationKeyField
) {
  try {
    log(`Setting replication key state after initial sync`);
    const row = await destKnex
      .withSchema(`${process.env.DEST_SCHEMA}`)
      .table(tableName)
      .max(replicationKeyField);
    saveReplicationKeyStateToFile(
      row.length && row[0].max
        ? row[0].max.toISOString()
        : new Date(0).toISOString(),
      tableName
    );
  } catch (error) {
    log(
      `SetReplicationKeyStateAfterInitialSync function error ${error.stack}`,
      true
    );
    throw error;
  }
}

async function doPgDumpRestoreForTable(tableName) {
  try {
    log(`Doing pgdump-restore`);
    const pgDump = cp.spawnSync(
      "pg_dump",
      [
        "-h",
        process.env.SOURCE_HOST,
        "-U",
        process.env.SOURCE_USERNAME,
        process.env.SOURCE_DB,
        "-f",
        "./dump.sql",
        "--data-only",
        "--table",
        `${process.env.SOURCE_SCHEMA}.${tableName}`,
      ],
      {
        env: { ...process.env, PGPASSWORD: process.env.SOURCE_PASSWORD },
      }
    );
    log(`Pgdump stdout ${pgDump.stdout}`);
    log(`Pgdump stderr ${pgDump.stderr}`);

    const sed = cp.spawnSync("sed", ["-i", "", "/setval/d", "./dump.sql"]);
    log(`Sed stdout ${sed.stdout}`);
    log(`Sed stderr ${sed.stderr}`);

    const psql = cp.spawnSync(
      "psql",
      [
        "-h",
        process.env.DEST_HOST,
        "-U",
        process.env.DEST_USERNAME,
        "-d",
        process.env.DEST_DB,
        "-f",
        "./dump.sql",
      ],
      {
        env: { ...process.env, PGPASSWORD: process.env.DEST_PASSWORD },
      }
    );
    log(`Psql stdout ${psql.stdout}`);
    log(`Psql stderr ${psql.stderr}`);
  } catch (error) {
    log(`DoPgDumpRestoreForTable function error ${error.stack}`, true);
    throw error;
  }
}

async function deduplicateRowsForTableInDestination(uniqueCols, tableName) {
  try {
    log("Deduplicating rows in destination final using destination staging");
    const whereClause = uniqueCols
      .map(
        (key) =>
          `${process.env.DEST_SCHEMA}.${tableName}.${key} = ${process.env.DEST_SCHEMA}.${tableName}_stg.${key}`
      )
      .join(" AND ");
    await destKnex.raw(`
        DELETE FROM ${process.env.DEST_SCHEMA}.${tableName}
        WHERE EXISTS (
          SELECT 1
          FROM ${process.env.DEST_SCHEMA}.${tableName}_stg
          WHERE ${whereClause}
        )
      `);
    await destKnex.raw(`
        INSERT INTO ${process.env.DEST_SCHEMA}.${tableName}
        SELECT *
        FROM ${process.env.DEST_SCHEMA}.${tableName}_stg
      `);
    await destKnex.raw(`
        DELETE FROM ${process.env.DEST_SCHEMA}.${tableName}_stg
      `);
  } catch (error) {
    log(
      `DeduplicateRowsForTableInDestination function error ${error.stack}`,
      true
    );
    throw error;
  }
}

function saveReplicationKeyStateToFile(replicationKey, tableName) {
  try {
    log("Saving replication key state to file");
    let temp = new Date(replicationKey);
    temp = new Date(temp.getTime() + 1);
    temp = temp.toISOString();
    fs.writeFileSync(
      path.resolve(__dirname, `./sync-state/${tableName}.txt`),
      temp,
      "utf8"
    );
  } catch (error) {
    log(`SaveReplicationKeyStateToFile function error ${error.stack}`, true);
    throw error;
  }
}

async function performIncrementalReplicationForTable(
  tableName,
  replicationKeyField,
  replicationKeyState
) {
  try {
    log("Performing incremental replication for table");
    await destKnex.raw(`
        DELETE FROM ${process.env.DEST_SCHEMA}.${tableName}_stg
      `);
    let offset = 0;
    let data, lastRow;
    const batchSize = 1000;
    let batchNo = 0;
    while (true) {
      data = await srcKnex
        .withSchema(process.env.SOURCE_SCHEMA)
        .table(tableName)
        .where(replicationKeyField, ">=", replicationKeyState)
        .orderBy(replicationKeyField)
        .offset(offset)
        .limit(batchSize)
        .select();

      if (!data.length) {
        log(`All modified rows replicated to destination staging`);
        break;
      }
      // const serializedData = data.map((row) => {
      //   return Object.entries(row).reduce((acc, [key, value]) => {
      //     if (typeof value === "object" && value !== null) {
      //       if (value instanceof Date) {
      //         acc[key] = value.toISOString();
      //       }

      //       if (Array.isArray(value)) {
      //         acc[key] = value;
      //       } else {
      //         acc[key] = JSON.stringify(value);
      //       }
      //     } else {
      //       acc[key] = value;
      //     }
      //     return acc;
      //   }, {});
      // });
      serializedData = data;
      await destKnex
        .withSchema(process.env.DEST_SCHEMA)
        .table(`${tableName}_stg`)
        .insert(serializedData);
      offset += batchSize;
      lastRow = data[data.length - 1];
      batchNo++;
      log(`Replicated ${data.length} rows in batch number ${batchNo}`);
    }

    return lastRow ? lastRow[replicationKeyField].toISOString() : null;
  } catch (error) {
    log(
      `PerformIncrementalReplicationForTable function error ${error.stack}`,
      true
    );
    throw error;
  }
}

function loadReplicationKeyStateFromFile(tableName) {
  try {
    log("Loading replication key state from file");
    const replicationKey = fs.readFileSync(
      path.resolve(__dirname, `./sync-state/${tableName}.txt`),
      "utf8"
    );
    return new Date(replicationKey);
  } catch (error) {
    log(`LoadReplicationKeyStateFromFile function error ${error.stack}`);
    throw error;
  }
}

const resetDestinationSchemaForTable = async (tableName) => {
  try {
    log(`Resetting destination table schema`);
    const tableExists = await destKnex.schema
      .withSchema(process.env.DEST_SCHEMA)
      .hasTable(tableName);
    const tableExistsStg = await destKnex.schema
      .withSchema(process.env.DEST_SCHEMA)
      .hasTable(`${tableName}_stg`);
    if (tableExists) {
      await destKnex.schema
        .withSchema(process.env.DEST_SCHEMA)
        .dropTable(tableName);
    }
    if (tableExistsStg) {
      await destKnex.schema
        .withSchema(process.env.DEST_SCHEMA)
        .dropTable(`${tableName}_stg`);
    }
    const srcColumnInfoQuery = `
    SELECT column_name, data_type, is_nullable, udt_name
    FROM information_schema.columns
    WHERE table_schema = '${process.env.SOURCE_SCHEMA}'
    AND table_name = '${tableName}';
  `;
    const srcColumnInfo = await srcKnex.raw(srcColumnInfoQuery);
    let queryStg = `CREATE TABLE "${process.env.DEST_SCHEMA}"."${tableName}_stg" (`;
    let query = `CREATE TABLE "${process.env.DEST_SCHEMA}"."${tableName}" (`;
    const columns = srcColumnInfo.rows.map((columnData) => {
      const { column_name, data_type, is_nullable, udt_name } = columnData;
      let columnDefinition = `"${column_name}" ${
        data_type === "ARRAY"
          ? `${udt_name.replace("_", "")}[]`
          : data_type === "USER-DEFINED"
          ? "text"
          : data_type
      }`;
      if (is_nullable === "NO") {
        columnDefinition += " NOT NULL";
      }
      return columnDefinition;
    });

    query += columns.join(", ");
    query += ")";

    queryStg += columns.join(", ");
    queryStg += ")";

    await destKnex.raw(query);
    await destKnex.raw(queryStg);
  } catch (error) {
    log(`ResetDestinationSchemaForTable function error ${error.stack}`, true);
    throw error;
  }
};

const areSchemasSameForTable = async (tableName) => {
  log(`Checking if schemas are equal`);
  try {
    const srcSchema = await srcKnex
      .withSchema(process.env.SOURCE_SCHEMA)
      .table(tableName)
      .columnInfo();
    const destStagingSchema = await destKnex
      .withSchema(process.env.DEST_SCHEMA)
      .table(`${tableName}_stg`)
      .columnInfo();
    const srcColumns = Object.keys(srcSchema);
    const destStgCols = Object.keys(destStagingSchema);
    if (srcColumns.length !== destStgCols.length) {
      return false;
    }
    for (let columnName of srcColumns) {
      if (
        !destStagingSchema[columnName] ||
        (srcSchema[columnName].type !== destStagingSchema[columnName].type &&
          srcSchema[columnName].type !== "USER-DEFINED" &&
          destStagingSchema[columnName].type !== "text")
      ) {
        console.log(
          `${srcSchema[columnName].type} ${destStagingSchema[columnName].type}`
        );
        return false;
      }
    }
    return true;
  } catch (error) {
    log(`AreSchemasSameForTable function error ${error.stack}`, true);
    throw error;
  }
};

main();
