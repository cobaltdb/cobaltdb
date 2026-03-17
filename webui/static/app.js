// Monaco Editor instance
let editor;
let currentQuery = '';

// Tabs management
let tabs = [{ id: 0, name: 'Query 1', content: "-- Welcome to CobaltDB Web UI\n-- Write your SQL queries here\n\nSELECT * FROM sqlite_master WHERE type='table';" }];
let activeTabId = 0;
let nextTabId = 1;

// Initialize Monaco Editor
require.config({
    paths: {
        'vs': 'https://cdn.jsdelivr.net/npm/monaco-editor@0.44.0/min/vs'
    }
});

require(['vs/editor/editor.main'], function() {
    // Register SQL language
    monaco.editor.defineTheme('cobaltDark', {
        base: 'vs-dark',
        inherit: true,
        rules: [
            { token: 'keyword', foreground: '60A5FA', fontStyle: 'bold' },
            { token: 'identifier', foreground: 'F472B6' },
            { token: 'string', foreground: 'A3E635' },
            { token: 'number', foreground: 'FBBF24' },
            { token: 'comment', foreground: '6B7280' },
        ],
        colors: {
            'editor.background': '#0F172A',
            'editor.lineHighlightBackground': '#1E293B',
            'editor.selectionBackground': '#3B82F680',
        }
    });

    // Create editor
    editor = monaco.editor.create(document.getElementById('editor'), {
        value: "-- Welcome to CobaltDB Web UI\n-- Write your SQL queries here\n\nSELECT * FROM sqlite_master WHERE type='table';",
        language: 'sql',
        theme: 'cobaltDark',
        fontSize: 14,
        fontFamily: 'Monaco, Menlo, "Ubuntu Mono", monospace',
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        automaticLayout: true,
        padding: { top: 20 },
        lineNumbers: 'on',
        roundedSelection: true,
        scrollbar: {
            useShadows: false,
            verticalHasArrows: false,
            horizontalHasArrows: false,
            vertical: 'visible',
            horizontal: 'visible'
        }
    });

    // Add keyboard shortcut
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, function() {
        runQuery();
    });

    // Focus editor
    editor.focus();
});

// Run query
document.getElementById('runBtn').addEventListener('click', runQuery);

// Format SQL
document.getElementById('formatBtn').addEventListener('click', function() {
    if (editor) {
        editor.getAction('editor.action.formatDocument').run();
    }
});

// Clear editor
document.getElementById('clearBtn').addEventListener('click', function() {
    if (editor) {
        editor.setValue('');
        editor.focus();
    }
});

// Export buttons
document.getElementById('exportCsvBtn').addEventListener('click', function() {
    if (currentQuery) {
        window.open('/api/export/csv?query=' + encodeURIComponent(currentQuery), '_blank');
    }
});

document.getElementById('exportJsonBtn').addEventListener('click', function() {
    if (currentQuery) {
        window.open('/api/export/json?query=' + encodeURIComponent(currentQuery), '_blank');
    }
});

// Save Query button
document.getElementById('saveQueryBtn').addEventListener('click', function() {
    if (!editor) return;
    const query = editor.getValue().trim();
    if (!query) {
        alert('Please enter a query first');
        return;
    }
    openSaveModal();
});

// Save Query Form
document.getElementById('saveQueryForm').addEventListener('submit', async function(e) {
    e.preventDefault();

    const name = document.getElementById('queryName').value.trim();
    const description = document.getElementById('queryDescription').value.trim();
    const query = editor.getValue().trim();

    if (!name || !query) return;

    try {
        const response = await fetch('/api/saved-queries', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                name: name,
                query: query,
                description: description
            })
        });

        if (response.ok) {
            closeSaveModal();
            loadSavedQueries();
            document.getElementById('saveQueryForm').reset();
        } else {
            const error = await response.text();
            alert('Failed to save query: ' + error);
        }
    } catch (error) {
        alert('Failed to save query: ' + error.message);
    }
});

// Run query function
async function runQuery() {
    if (!editor) return;

    const query = editor.getValue().trim();
    if (!query) return;

    currentQuery = query;

    // Update UI
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Running query...</div>';

    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: query })
        });

        const data = await response.json();

        if (data.success) {
            displayResults(data);
            loadHistory();
        } else {
            displayError(data.message);
        }

        // Enable export buttons
        document.getElementById('exportCsvBtn').disabled = !data.success || data.rowCount === 0;
        document.getElementById('exportJsonBtn').disabled = !data.success || data.rowCount === 0;

    } catch (error) {
        displayError(error.message);
    }
}

// Display query results
function displayResults(data) {
    const resultsDiv = document.getElementById('results');
    const rowCountSpan = document.getElementById('rowCount');
    const execTimeSpan = document.getElementById('execTime');

    // Update info
    rowCountSpan.textContent = `${data.rowCount} rows`;
    execTimeSpan.textContent = data.duration;

    // Check if it's a non-SELECT result
    if (data.columns.length === 1 && data.columns[0] === 'Result') {
        resultsDiv.innerHTML = `
            <div class="success-message">
                <i class="fas fa-check-circle"></i> ${data.rows[0][0]}
            </div>
        `;
        return;
    }

    // Create results table
    if (data.rows.length === 0) {
        resultsDiv.innerHTML = `
            <div class="empty-state">
                <i class="fas fa-inbox"></i>
                <p>Query returned no rows</p>
            </div>
        `;
        return;
    }

    let html = '<table class="data-table"><thead><tr>';

    // Headers
    data.columns.forEach(col => {
        html += `<th>${escapeHtml(col)}</th>`;
    });
    html += '</tr></thead><tbody>';

    // Rows
    data.rows.forEach(row => {
        html += '<tr>';
        row.forEach(cell => {
            if (cell === null) {
                html += '<td class="null-value">NULL</td>';
            } else {
                html += `<td>${escapeHtml(String(cell))}</td>`;
            }
        });
        html += '</tr>';
    });

    html += '</tbody></table>';
    resultsDiv.innerHTML = html;

    // Add inline editing
    enableInlineEditing(data.columns, data.rows, currentQuery);
}

// Display error
function displayError(message) {
    const resultsDiv = document.getElementById('results');
    const rowCountSpan = document.getElementById('rowCount');
    const execTimeSpan = document.getElementById('execTime');

    rowCountSpan.textContent = 'Error';
    execTimeSpan.textContent = '';

    resultsDiv.innerHTML = `
        <div class="error-message">
            <h3><i class="fas fa-exclamation-triangle"></i> Query Error</h3>
            <pre>${escapeHtml(message)}</pre>
        </div>
    `;
}

// Load schema
async function loadSchema() {
    try {
        const response = await fetch('/api/schema');
        const data = await response.json();

        const tableList = document.getElementById('tableList');

        if (data.tables.length === 0) {
            tableList.innerHTML = '<div class="empty">No tables</div>';
            return;
        }

        let html = '';
        data.tables.forEach(table => {
            html += `
                <div class="table-item" onclick="showTableInfo('${escapeHtml(table.name)}')">
                    <i class="fas fa-table"></i>
                    <span class="table-name">${escapeHtml(table.name)}</span>
                </div>
            `;
        });

        tableList.innerHTML = html;
    } catch (error) {
        console.error('Failed to load schema:', error);
    }
}

// Show table info modal
async function showTableInfo(tableName) {
    try {
        const response = await fetch('/api/tables/' + encodeURIComponent(tableName));
        const data = await response.json();

        document.getElementById('modalTitle').textContent = 'Table: ' + tableName;

        let html = `
            <table class="schema-table">
                <thead>
                    <tr>
                        <th>Column</th>
                        <th>Type</th>
                    </tr>
                </thead>
                <tbody>
        `;

        data.columns.forEach(col => {
            html += `
                <tr>
                    <td><strong>${escapeHtml(col.name)}</strong></td>
                    <td>${escapeHtml(col.type)}</td>
                </tr>
            `;
        });

        html += '</tbody></table>';

        // Add quick actions
        html += `
            <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid var(--border-color);">
                <h4 style="margin-bottom: 10px; color: var(--text-secondary);">Quick Actions</h4>
                <button class="btn btn-secondary" onclick="setQuery('SELECT * FROM ${escapeHtml(tableName)} LIMIT 100')">
                    <i class="fas fa-search"></i> Preview Data
                </button>
                <button class="btn btn-secondary" onclick="setQuery('SELECT COUNT(*) FROM ${escapeHtml(tableName)}')">
                    <i class="fas fa-calculator"></i> Count Rows
                </button>
            </div>
        `;

        document.getElementById('modalBody').innerHTML = html;
        document.getElementById('tableModal').classList.add('active');
    } catch (error) {
        console.error('Failed to load table info:', error);
    }
}

// Close modal
function closeModal() {
    document.getElementById('tableModal').classList.remove('active');
}

// Set query in editor
function setQuery(query) {
    if (editor) {
        editor.setValue(query);
        editor.focus();
        closeModal();
    }
}

// Load query history
async function loadHistory() {
    try {
        const response = await fetch('/api/history');
        const data = await response.json();

        const historyList = document.getElementById('historyList');

        if (data.length === 0) {
            historyList.innerHTML = '<div class="empty">No queries yet</div>';
            return;
        }

        let html = '';
        data.forEach(item => {
            const time = new Date(item.timestamp).toLocaleTimeString();
            html += `
                <div class="history-item" onclick="setQuery(this.dataset.query)" data-query="${escapeHtml(item.query)}">
                    <div class="history-query">${escapeHtml(item.query.substring(0, 50))}${item.query.length > 50 ? '...' : ''}</div>
                    <div class="history-meta">
                        <span><i class="fas fa-clock"></i> ${time}</span>
                        <span><i class="fas fa-bolt"></i> ${item.duration}</span>
                        <span><i class="fas fa-list-ol"></i> ${item.rows} rows</span>
                    </div>
                </div>
            `;
        });

        historyList.innerHTML = html;
    } catch (error) {
        console.error('Failed to load history:', error);
    }
}

// Enable inline editing on data table
function enableInlineEditing(columns, rows, query) {
    // Only enable editing for simple SELECT * FROM table queries
    const tableMatch = query.match(/FROM\s+(\w+)/i);
    if (!tableMatch) return;

    const tableName = tableMatch[1];
    const table = document.querySelector('.data-table');
    if (!table) return;

    const dataCells = table.querySelectorAll('tbody td');
    dataCells.forEach((cell, index) => {
        const rowIndex = Math.floor(index / columns.length);
        const colIndex = index % columns.length;
        const columnName = columns[colIndex];

        // Skip if primary key column (for WHERE clause)
        if (columnName.toLowerCase() === 'id' || columnName.toLowerCase().endsWith('_id')) {
            return;
        }

        cell.classList.add('editable');
        cell.addEventListener('click', function() {
            editCell(this, tableName, columnName, columns, rows[rowIndex]);
        });
    });
}

// Edit a cell
function editCell(cell, tableName, columnName, allColumns, rowData) {
    // If already editing, return
    if (cell.querySelector('input')) return;

    const currentValue = cell.textContent === 'NULL' ? '' : cell.textContent;
    const input = document.createElement('input');
    input.type = 'text';
    input.value = currentValue;
    input.className = 'edit-input';

    // Save on blur or enter
    function save() {
        const newValue = input.value;
        cell.textContent = newValue || 'NULL';

        // Build WHERE clause from row data
        const where = {};
        allColumns.forEach((col, idx) => {
            if (col.toLowerCase() === 'id' || col.toLowerCase().endsWith('_id')) {
                where[col] = rowData[idx];
            }
        });

        // If no ID column found, can't update
        if (Object.keys(where).length === 0) {
            alert('Cannot update: No ID column found');
            return;
        }

        // Send update to server
        updateRow(tableName, columnName, newValue, where);
    }

    input.addEventListener('blur', save);
    input.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            input.blur();
        } else if (e.key === 'Escape') {
            cell.textContent = currentValue || 'NULL';
        }
    });

    cell.textContent = '';
    cell.appendChild(input);
    input.focus();
    input.select();
}

// Update row on server
async function updateRow(table, column, value, where) {
    try {
        const response = await fetch('/api/update-row', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                table: table,
                column: column,
                value: value,
                where: where
            })
        });

        const data = await response.json();
        if (!data.success) {
            alert('Update failed: ' + data.error);
        }
    } catch (error) {
        console.error('Failed to update:', error);
        alert('Update failed: ' + error.message);
    }
}

// Escape HTML
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Close modal on outside click
document.getElementById('tableModal').addEventListener('click', function(e) {
    if (e.target === this) {
        closeModal();
    }
});

// Keyboard shortcuts
document.addEventListener('keydown', function(e) {
    // Ctrl/Cmd + / to format
    if ((e.ctrlKey || e.metaKey) && e.key === '/') {
        e.preventDefault();
        if (editor) {
            editor.getAction('editor.action.formatDocument').run();
        }
    }

    // Escape to close modals
    if (e.key === 'Escape') {
        closeModal();
        closeSaveModal();
    }
});

// Load saved queries
async function loadSavedQueries() {
    try {
        const response = await fetch('/api/saved-queries');
        const data = await response.json();

        const list = document.getElementById('savedQueriesList');

        if (data.length === 0) {
            list.innerHTML = '<div class="empty">No saved queries</div>';
            return;
        }

        let html = '';
        data.forEach(item => {
            html += `
                <div class="saved-query-item">
                    <div class="query-info" onclick="loadSavedQuery('${escapeHtml(item.name)}')">
                        <div class="query-name">${escapeHtml(item.name)}</div>
                        ${item.description ? `<div class="query-desc">${escapeHtml(item.description)}</div>` : ''}
                    </div>
                    <button class="delete-btn" onclick="deleteSavedQuery('${escapeHtml(item.name)}', event)" title="Delete">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            `;
        });

        list.innerHTML = html;
    } catch (error) {
        console.error('Failed to load saved queries:', error);
    }
}

// Load a saved query into the editor
async function loadSavedQuery(name) {
    try {
        const response = await fetch('/api/saved-queries/' + encodeURIComponent(name));
        if (response.ok) {
            const data = await response.json();
            setQuery(data.query);
        }
    } catch (error) {
        console.error('Failed to load saved query:', error);
    }
}

// Delete a saved query
async function deleteSavedQuery(name, event) {
    event.stopPropagation();

    if (!confirm(`Delete saved query "${name}"?`)) {
        return;
    }

    try {
        const response = await fetch('/api/saved-queries/' + encodeURIComponent(name), {
            method: 'DELETE'
        });

        if (response.ok) {
            loadSavedQueries();
        }
    } catch (error) {
        console.error('Failed to delete saved query:', error);
    }
}

// Export saved queries
function exportSavedQueries() {
    window.open('/api/export-saved-queries', '_blank');
}

// Import saved queries
async function importSavedQueries(input) {
    const file = input.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    try {
        const response = await fetch('/api/import-saved-queries', {
            method: 'POST',
            body: formData
        });

        if (response.ok) {
            const result = await response.json();
            alert(`Successfully imported ${result.count} queries`);
            loadSavedQueries();
        } else {
            const error = await response.text();
            alert('Failed to import: ' + error);
        }
    } catch (error) {
        alert('Failed to import: ' + error.message);
    }

    // Reset input
    input.value = '';
}

// Modal functions
function openSaveModal() {
    document.getElementById('saveQueryModal').classList.add('active');
    document.getElementById('queryName').focus();
}

function closeSaveModal() {
    document.getElementById('saveQueryModal').classList.remove('active');
}

// Close save modal on outside click
document.getElementById('saveQueryModal').addEventListener('click', function(e) {
    if (e.target === this) {
        closeSaveModal();
    }
});

// Theme toggle
document.getElementById('themeToggle').addEventListener('click', function() {
    const html = document.documentElement;
    const currentTheme = html.getAttribute('data-theme');
    const newTheme = currentTheme === 'light' ? 'dark' : 'light';

    html.setAttribute('data-theme', newTheme === 'dark' ? '' : 'light');
    localStorage.setItem('theme', newTheme);

    // Update Monaco Editor theme
    if (editor) {
        monaco.editor.setTheme(newTheme === 'light' ? 'vs' : 'cobaltDark');
    }
});

// Load saved theme
function loadTheme() {
    const savedTheme = localStorage.getItem('theme') || 'dark';
    if (savedTheme === 'light') {
        document.documentElement.setAttribute('data-theme', 'light');
    }
    return savedTheme;
}

// Tab functions
function addTab() {
    if (editor) {
        const currentTab = tabs.find(t => t.id === activeTabId);
        if (currentTab) {
            currentTab.content = editor.getValue();
        }
    }

    const newId = nextTabId++;
    tabs.push({
        id: newId,
        name: `Query ${newId + 1}`,
        content: ''
    });

    switchTab(newId);
}

function closeTab(id, event) {
    event.stopPropagation();

    if (tabs.length === 1) {
        alert('Cannot close the last tab');
        return;
    }

    const index = tabs.findIndex(t => t.id === id);
    if (index === -1) return;

    tabs.splice(index, 1);

    if (id === activeTabId) {
        const newActive = tabs[Math.max(0, index - 1)];
        switchTab(newActive.id);
    } else {
        renderTabs();
    }
}

function switchTab(id) {
    if (editor) {
        const currentTab = tabs.find(t => t.id === activeTabId);
        if (currentTab) {
            currentTab.content = editor.getValue();
        }
    }

    activeTabId = id;
    const tab = tabs.find(t => t.id === id);
    if (tab && editor) {
        editor.setValue(tab.content);
    }

    renderTabs();
}

function renderTabs() {
    const container = document.getElementById('queryTabs');
    if (!container) return;

    container.innerHTML = tabs.map(tab => `
        <div class="tab ${tab.id === activeTabId ? 'active' : ''}" data-tab="${tab.id}" onclick="switchTab(${tab.id})">
            <span class="tab-name" ondblclick="renameTab(${tab.id})" title="Double-click to rename">${escapeHtml(tab.name)}</span>
            ${tabs.length > 1 ? `<button class="tab-close" onclick="closeTab(${tab.id}, event)"><i class="fas fa-times"></i></button>` : ''}
        </div>
    `).join('');
}

function renameTab(id) {
    const tab = tabs.find(t => t.id === id);
    if (!tab) return;

    const newName = prompt('Enter tab name:', tab.name);
    if (newName && newName.trim()) {
        tab.name = newName.trim();
        renderTabs();
    }
}

// Initialize on load
window.addEventListener('DOMContentLoaded', function() {
    loadTheme();
    loadSchema();
    loadHistory();
    loadSavedQueries();
    renderTabs();
});
