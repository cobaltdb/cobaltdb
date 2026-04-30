declare module 'sql.js' {
  type SqlValue = string | number | Uint8Array | null

  interface SqlJsStatic {
    Database: new (data?: ArrayLike<number> | Uint8Array | null) => Database
  }

  interface Database {
    run(sql: string, params?: SqlValue[]): Database
    exec(sql: string, params?: SqlValue[]): QueryExecResult[]
    getRowsModified(): number
    close(): void
    export(): Uint8Array
  }

  interface QueryExecResult {
    columns: string[]
    values: SqlValue[][]
  }

  interface SqlJsConfig {
    locateFile?: (file: string) => string
  }

  export default function initSqlJs(config?: SqlJsConfig): Promise<SqlJsStatic>
  export type { Database, QueryExecResult, SqlJsStatic, SqlValue }
}
