/// <reference path="./hana.d.ts" />

/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `SapHanaDriver` and related types declaration.
 */

import { getEnv, assertDataSource } from '@cubejs-backend/shared';
import genericPool, { type Pool } from 'generic-pool';
import { promisify } from 'util';
import {
  BaseDriver,
  type GenericDataBaseType,
  type DriverInterface,
  type DownloadQueryResultsOptions,
  type DownloadQueryResultsResult,
  type StreamOptions,
  type QueryOptions,
} from '@cubejs-backend/base-driver';

// eslint-disable-next-line @typescript-eslint/no-require-imports
import hanaClient = require('@sap/hana-client');
// eslint-disable-next-line @typescript-eslint/no-require-imports
import TypeCode = require('@sap/hana-client/extension/TypeCode');
// eslint-disable-next-line @typescript-eslint/no-require-imports
import Stream = require('@sap/hana-client/extension/Stream');

interface HanaConnectionOptions {
  host?: string;
  port?: number;
  serverNode?: string;
  uid?: string;
  pwd?: string;
  schema?: string;
  databaseName?: string;
  autoCommit?: boolean;
  ca?: string;
  encrypt?: boolean;
  sslValidateCertificate?: boolean;
}

interface HanaFieldInfo {
  columnName: string;
  originalColumnName: string;
  tableName: string;
  ownerName: string;
  type: number;
  typeName: string;
  nativeType: number;
  nativeTypeName: string;
  precision: number;
  scale: number;
  nullable: number;
}

interface HanaResultSet {
  next(): boolean;
  getValues<T = Record<string, unknown>>(): T;
  getColumnInfo(): HanaFieldInfo[];
}

interface HanaStatement {
  exec(fn: (error: Error | null, rows: unknown[]) => void): void;
  exec(params: unknown[], fn: (error: Error | null, rows: unknown[]) => void): void;
  execQuery(params: unknown[], fn: (error: Error | null, rs: HanaResultSet) => void): void;
  execQuery(params: unknown[]): HanaResultSet;
}

interface HanaRawConnection {
  connect: (opts: HanaConnectionOptions, cb: (err: Error | null) => void) => void;
  exec: (
    sql: string,
    params: unknown[] | ((err: Error | null, rows: unknown[]) => void),
    cb?: (err: Error | null, rows: unknown[]) => void,
  ) => void;
  end: (cb: (err: Error | null) => void) => void;
  prepare: (sql: string) => HanaStatement;
  on?: (event: string, cb: () => void) => void;
  execute?: (sql: string, params?: unknown[]) => Promise<unknown[]>;
  destroy?: () => void;
}

interface HanaPoolConnection extends HanaRawConnection {
  execute: (sql: string, params?: unknown[]) => Promise<unknown[]>;
}

// convert HANA build-in types with type:type object
const HanaBuildInTypes: Record<string, string> = {};
Object.entries(TypeCode).forEach(([key]) => {
  HanaBuildInTypes[key] = key;
});

const SapHanaToGenericType: Record<string, GenericDataBaseType> = {
  smalldecimal: 'decimal',
  seconddate: 'timestamp',
  daydate: 'date',
  smallint: 'int',
  bigint: 'int',
  tinyint: 'int',
};

export interface SapHanaDriverConfiguration {
  host?: string;
  port?: number;
  serverNode?: string;
  uid?: string;
  pwd?: string;
  schema?: string;
  databaseName?: string;
  autoCommit?: boolean;
  ca?: string;
  encrypt?: boolean;
  sslValidateCertificate?: boolean;
  readOnly?: boolean;
  loadPreAggregationWithoutMetaLock?: boolean;
  storeTimezone?: string;
  currentSchema?: string;
  pool?: Partial<genericPool.Options>;
}

/**
 * SAP HANA driver class.
 */
export class SapHanaDriver extends BaseDriver implements DriverInterface {
  /**
   * Returns default concurrency value.
   */
  public static getDefaultConcurrency(): number {
    return 2;
  }

  protected readonly config: SapHanaDriverConfiguration;

  protected pool: Pool<HanaPoolConnection>;

  /**
   * Class constructor.
   */
  public constructor(
    config: SapHanaDriverConfiguration & {
      dataSource?: string;
      maxPoolSize?: number;
    } = {},
  ) {
    super();

    const dataSource = config.dataSource || assertDataSource('default');

    const { pool, host: configHost, port: configPort, ...restConfig } = config;
    const host = configHost || getEnv('dbHost', [{ dataSource }]);
    const port = configPort || getEnv('dbPort', [{ dataSource }]);
    this.config = {
      serverNode: port ? `${host}:${port}` : host,
      uid: getEnv('dbUser', [{ dataSource }]),
      pwd: getEnv('dbPass', [{ dataSource }]),
      encrypt: true,
      sslValidateCertificate: true,
      readOnly: true,
      ...restConfig,
    };
    this.pool = genericPool.createPool(
      {
        create: async () => {
          const raw = hanaClient.createConnection() as HanaRawConnection;
          const connect = promisify(raw.connect.bind(raw)) as (
            opts: SapHanaDriverConfiguration,
          ) => Promise<void>;
          if (raw.on) {
            raw.on('error', () => {
              raw.destroy?.();
            });
          }
          const poolConn = raw as HanaPoolConnection;
          poolConn.execute = promisify(raw.exec.bind(raw)) as (
            sql: string,
            params?: unknown[],
          ) => Promise<unknown[]>;
          await connect(this.config);
          return poolConn;
        },
        validate: async (connection) => {
          try {
            await connection.execute('SELECT 1 FROM DUMMY');
          } catch (e) {
            this.databasePoolError(e);
            return false;
          }
          return true;
        },
        destroy: (connection) => promisify(connection.end.bind(connection))(),
      },
      {
        min: 0,
        max: config.maxPoolSize || getEnv('dbMaxPoolSize', [{ dataSource }]) || 8,
        evictionRunIntervalMillis: 10000,
        softIdleTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        testOnBorrow: true,
        acquireTimeoutMillis: 20000,
        ...pool,
      },
    );
  }

  public readOnly() {
    return !!this.config.readOnly;
  }

  protected async getConnectionFromPool(): Promise<HanaPoolConnection> {
    return this.pool.acquire();
  }

  public async testConnection(): Promise<void> {
    const conn = await this.getConnectionFromPool();
    try {
      await conn.execute('SELECT 1 FROM DUMMY');
    } finally {
      await this.pool.release(conn);
    }
  }

  public async query<R = unknown>(
    query: string,
    values: unknown[],
    _options?: QueryOptions,
  ): Promise<R[]> {
    const conn = await this.getConnectionFromPool();
    try {
      return (await conn.execute(query, values || [])) as R[];
    } finally {
      await this.pool.release(conn);
    }
  }

  protected queryResultSet(conn: HanaPoolConnection, query: string, values: unknown[]): HanaResultSet {
    const stmt = conn.prepare(query);
    return stmt.execQuery(values);
  }

  public async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  public informationSchemaQuery() {
    return `
      SELECT columns.COLUMN_NAME as ${this.quoteIdentifier('column_name')},
             columns.TABLE_NAME as ${this.quoteIdentifier('table_name')},
             columns.SCHEMA_NAME as ${this.quoteIdentifier('table_schema')},
             columns.DATA_TYPE_NAME as ${this.quoteIdentifier('data_type')}
      FROM SYS.TABLE_COLUMNS columns
      WHERE columns.SCHEMA_NAME = '${this.config.currentSchema || this.config.uid}'
   `;
  }

  public quoteIdentifier(identifier: string) {
    return `"${identifier}"`;
  }

  public loadPreAggregationIntoTable(
    preAggregationTableName: string,
    loadSql: string,
    params: unknown[],
    tx: unknown,
  ) {
    if (this.config.loadPreAggregationWithoutMetaLock) {
      return this.cancelCombinator(
        async (saveCancelFn: (p: Promise<unknown>) => Promise<unknown>) => {
          await saveCancelFn(this.query(`${loadSql} LIMIT 0`, params));
          await saveCancelFn(
            this.query(loadSql.replace(/^CREATE TABLE (\S+) AS/i, 'INSERT INTO $1'), params),
          );
        },
      );
    }

    return super.loadPreAggregationIntoTable(preAggregationTableName, loadSql, params, tx);
  }

  public async stream(query: string, values: unknown[], _: StreamOptions) {
    const conn = await this.getConnectionFromPool();
    try {
      const resultSet = this.queryResultSet(conn, query, values);
      const columnInfo = resultSet.getColumnInfo();
      const rowStream = Stream.createObjectStream(resultSet);
      return {
        rowStream,
        types: this.mapFieldsToGenericTypes(columnInfo),
        release: async () => {
          await this.pool.release(conn);
        },
      };
    } catch (e) {
      await this.pool.release(conn);
      throw e;
    }
  }

  public async downloadQueryResults(
    query: string,
    values: unknown[],
    options: DownloadQueryResultsOptions,
  ): Promise<DownloadQueryResultsResult> {
    if (options.streamImport) {
      return this.stream(query, values, options) as Promise<DownloadQueryResultsResult>;
    }
    const conn = await this.getConnectionFromPool();
    try {
      const resultSet = this.queryResultSet(conn, query, values);
      const rows: Record<string, unknown>[] = [];
      while (resultSet.next()) {
        rows.push(resultSet.getValues<Record<string, unknown>>());
      }
      return {
        rows,
        types: this.mapFieldsToGenericTypes(resultSet.getColumnInfo()),
      };
    } finally {
      await this.pool.release(conn);
    }
  }

  protected mapFieldsToGenericTypes(fields: HanaFieldInfo[]) {
    return fields.map((f) => {
      let hanaType = HanaBuildInTypes[f.nativeTypeName]?.toLowerCase() ?? '';

      if (f.nativeTypeName.toLowerCase() in SapHanaToGenericType) {
        hanaType = SapHanaToGenericType[f.nativeTypeName.toLowerCase()];
      }

      if (!hanaType) {
        throw new Error(
          `Unable to detect type for field "${f.columnName}" with dataTypeID: ${f.nativeTypeName}`,
        );
      }

      return {
        name: f.columnName,
        type: this.toGenericType(hanaType, f.precision ?? null, f.scale ?? null),
      };
    });
  }

  protected toGenericType(
    columnType: string,
    precision?: number | null,
    scale?: number | null,
  ): string {
    return (
      SapHanaToGenericType[columnType.toLowerCase()] ||
      SapHanaToGenericType[columnType.toLowerCase().split('(')[0]] ||
      super.toGenericType(columnType, precision, scale)
    );
  }

  protected getSchemasQuery(): string {
    return `SELECT SCHEMA_NAME AS schema_name FROM SYS.SCHEMAS WHERE HAS_PRIVILEGES = 'TRUE'`;
  }

  protected getTablesForSpecificSchemasQuery(schemasPlaceholders: string): string {
    return `SELECT SCHEMA_NAME AS schema_name, TABLE_NAME AS table_name
            FROM SYS.TABLES
            WHERE SCHEMA_NAME IN (${schemasPlaceholders})`;
  }

  protected getColumnsForSpecificTablesQuery(conditionString: string): string {
    return `SELECT SCHEMA_NAME AS schema_name, TABLE_NAME AS table_name,
                   COLUMN_NAME AS column_name, DATA_TYPE_NAME AS data_type
            FROM SYS.TABLE_COLUMNS
            WHERE ${conditionString}`;
  }

  protected getColumnNameForSchemaName(): string {
    return 'schema_name';
  }

  protected getColumnNameForTableName(): string {
    return 'table_name';
  }
}