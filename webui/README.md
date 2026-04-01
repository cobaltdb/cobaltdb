# CobaltDB Web UI

Modern, dark-themed web-based SQL editor for CobaltDB.

![Screenshot](https://via.placeholder.com/800x450/0f172a/3b82f6?text=CobaltDB+Web+UI)

## Features

- **SQL Editor**: Monaco Editor (VS Code's editor) with syntax highlighting
- **Schema Explorer**: Browse tables and their columns
- **Query History**: Recently executed queries with metadata
- **Saved Queries**: Save frequently used queries with descriptions
- **Results Display**: Data shown in formatted tables
- **Export**: CSV and JSON export functionality
- **Theme Toggle**: Dark/Light theme switching
- **Inline Editing**: Directly edit cell values in query results
- **Multiple Tabs**: Work with multiple queries simultaneously
- **Keyboard Shortcuts**:
  - `Ctrl+Enter` - Run query
  - `Ctrl+/` - Format SQL
  - `Escape` - Close modals
  - `Double-click tab` - Rename tab

## Quick Start

### Build (Important!)
Make sure to rebuild after any engine changes:
```bash
go build -o cobalt-webui.exe ./webui/server.go
```

**Note:** If you get "unsupported statement type" errors, the webui binary is outdated. Rebuild it with the command above.

### Run
```bash
./cobalt-webui.exe [flags] <database_file>
```

Example:
```bash
./cobalt-webui.exe mydb.db
# or for in-memory database:
./cobalt-webui.exe :memory:
```

### Access
By default Web UI binds to `127.0.0.1:8080` and enables token auth.

Start output includes a one-time URL like:

```text
Open http://127.0.0.1:8080/?token=<generated-token> in your browser
```

The token is converted to an HttpOnly cookie on first load.

### Security Flags

- `-addr` (default: `127.0.0.1:8080`) - HTTP bind address
- `-token` - explicit token value (or set `COBALTDB_WEBUI_TOKEN`)
- `-insecure-no-auth` - disable auth (unsafe; local trusted development only)

## Saved Queries

You can save frequently used queries for quick access:

1. Write your query in the editor
2. Click the **Save** button
3. Enter a name and optional description
4. Access saved queries from the sidebar

Saved queries are stored in memory and will be lost when the server restarts.

### Import/Export Saved Queries

You can export saved queries to a JSON file and import them later:

1. **Export**: Click the download icon next to "Saved Queries" header
2. **Import**: Click the upload icon and select a previously exported JSON file

## Theme Toggle

Switch between dark and light themes:

- Click the sun/moon icon in the database info section
- Theme preference is saved in browser localStorage
- Monaco Editor theme also adapts to the selected theme

## Inline Table Editing

Edit data directly in query results:

1. Run a `SELECT * FROM table` query
2. Click on any editable cell (non-ID columns)
3. Enter the new value and press Enter
4. The change is automatically saved to the database

**Note**: Inline editing requires an ID column (column named `id` or ending with `_id`) for the WHERE clause.

## Multiple Query Tabs

Work with multiple queries simultaneously:

- **New Tab**: Click the `+` button in the tabs header
- **Switch Tab**: Click on any tab to switch between queries
- **Close Tab**: Click the `×` button on a tab (cannot close the last tab)
- **Rename Tab**: Double-click on tab name to rename
- Tab content is preserved when switching between tabs

## API Endpoints

All API endpoints require auth when token auth is enabled (default).

- `GET /` - Web interface
- `POST /api/query` - Execute SQL query
- `GET /api/schema` - Get database schema
- `GET /api/history` - Get query history
- `GET /api/tables/<name>` - Get table info
- `GET /api/export/csv?query=<sql>` - Export to CSV
- `GET /api/export/json?query=<sql>` - Export to JSON
- `GET /api/saved-queries` - List saved queries
- `POST /api/saved-queries` - Save a query
- `GET /api/saved-queries/<name>` - Get a saved query
- `DELETE /api/saved-queries/<name>` - Delete a saved query
- `GET /api/export-saved-queries` - Export saved queries as JSON
- `POST /api/import-saved-queries` - Import saved queries from JSON
- `POST /api/update-row` - Update a row via inline editing

## Technology Stack

- **Backend**: Go with CobaltDB engine
- **Frontend**: Vanilla JavaScript
- **Editor**: Monaco Editor (SQL syntax highlighting)
- **Icons**: Font Awesome
- **Styling**: Custom CSS (dark/light themes)

## Directory Structure

```
webui/
├── server.go          # Go HTTP server
├── README.md          # This file
├── static/
│   ├── style.css     # Dark theme styles
│   └── app.js        # Frontend application
└── templates/
    └── index.html    # Main page template
```
