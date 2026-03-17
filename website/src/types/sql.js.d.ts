declare module 'sql.js' {
  interface SqlJsStatic {
    Database: new (data?: ArrayLike<number> | Buffer | null) => Database
  }

  interface Database {
    run(sql: string, params?: any[]): Database
    exec(sql: string, params?: any[]): QueryExecResult[]
    getRowsModified(): number
    close(): void
    export(): Uint8Array
  }

  interface QueryExecResult {
    columns: string[]
    values: any[][]
  }

  interface SqlJsConfig {
    locateFile?: (file: string) => string
  }

  export default function initSqlJs(config?: SqlJsConfig): Promise<SqlJsStatic>
  export type { Database, QueryExecResult, SqlJsStatic }
}
