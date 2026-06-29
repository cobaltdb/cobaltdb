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

// ---------------------------------------------------------------------------
// Admin panel: mint / rotate / revoke scoped tokens + audit log
// ---------------------------------------------------------------------------

// Detect the current principal and reveal the admin button only for admins.
async function initAdmin() {
    try {
        const res = await fetch('/api/me');
        if (!res.ok) return;
        const me = await res.json();
        if (me && me.isAdmin) {
            const btn = document.getElementById('adminBtn');
            if (btn) btn.style.display = '';
        }
    } catch (e) {
        // Non-fatal: if /api/me is unreachable, the panel simply stays hidden.
    }
}

function openAdminPanel() {
    document.getElementById('adminModal').classList.add('active');
    showAdminTab('tokens');
    loadAdminTokens();
}

function closeAdminPanel() {
    document.getElementById('adminModal').classList.remove('active');
}

function showAdminTab(which) {
    const onTokens = which === 'tokens';
    document.getElementById('adminTokensPane').style.display = onTokens ? '' : 'none';
    document.getElementById('adminAuditPane').style.display = onTokens ? 'none' : '';
    document.getElementById('adminTabTokens').classList.toggle('active', onTokens);
    document.getElementById('adminTabAudit').classList.toggle('active', !onTokens);
    if (!onTokens) loadAdminAudit();
}

async function loadAdminTokens() {
    const wrap = document.getElementById('adminTokensList');
    wrap.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';
    try {
        const res = await fetch('/api/admin/tokens');
        if (!res.ok) {
            wrap.innerHTML = '<div class="empty">Failed to load tokens (' + res.status + ')</div>';
            return;
        }
        const data = await res.json();
        const tokens = (data && data.tokens) || [];
        if (tokens.length === 0) {
            wrap.innerHTML = '<div class="empty">No tokens</div>';
            return;
        }
        let html = '<table class="admin-table"><thead><tr>' +
            '<th>Name</th><th>Role</th><th>Expires</th><th>Tables</th><th></th>' +
            '</tr></thead><tbody>';
        for (const t of tokens) {
            const isBootstrap = t.id === 'bootstrap';
            const expires = t.expires_at ? new Date(t.expires_at).toLocaleString() : 'never';
            const tables = (t.tables && t.tables.length) ? t.tables.join(', ') : '<span class="muted">all</span>';
            html += '<tr><td>' + escapeHtml(t.name) + '</td>' +
                '<td><span class="role-badge role-' + escapeHtml(t.role) + '">' + escapeHtml(t.role) + '</span></td>' +
                '<td>' + escapeHtml(expires) + '</td>' +
                '<td>' + tables + '</td><td class="admin-actions">';
            if (isBootstrap) {
                html += '<span class="muted">bootstrap</span>';
            } else {
                const id = escapeHtml(t.id);
                html += '<button class="btn btn-secondary btn-sm" onclick="rotateToken(\'' + id + '\')">Rotate</button> ' +
                    '<button class="btn btn-danger btn-sm" onclick="revokeToken(\'' + id + '\', \'' + escapeHtml(t.name) + '\')">Revoke</button>';
            }
            html += '</td></tr>';
        }
        html += '</tbody></table>';
        wrap.innerHTML = html;
    } catch (e) {
        wrap.innerHTML = '<div class="empty">Error: ' + escapeHtml(e.message) + '</div>';
    }
}

function showMintResult(message, token) {
    const box = document.getElementById('mintResult');
    box.style.display = '';
    if (token) {
        box.className = 'mint-result success';
        box.innerHTML = '<div><i class="fas fa-check-circle"></i> ' + escapeHtml(message) + '</div>' +
            '<div class="token-reveal"><code id="newTokenValue">' + escapeHtml(token) + '</code>' +
            '<button class="btn btn-secondary btn-sm" onclick="copyNewToken()"><i class="fas fa-copy"></i> Copy</button></div>' +
            '<div class="hint">Copy it now &mdash; it is shown only once and cannot be recovered.</div>';
    } else {
        box.className = 'mint-result error';
        box.innerHTML = '<i class="fas fa-exclamation-triangle"></i> ' + escapeHtml(message);
    }
}

function copyNewToken() {
    const el = document.getElementById('newTokenValue');
    if (el && navigator.clipboard) navigator.clipboard.writeText(el.textContent);
}

document.getElementById('mintTokenForm').addEventListener('submit', async function(e) {
    e.preventDefault();
    const name = document.getElementById('mintName').value.trim();
    const role = document.getElementById('mintRole').value;
    const ttl = document.getElementById('mintTTL').value.trim();
    const tablesRaw = document.getElementById('mintTables').value.trim();
    const tables = tablesRaw ? tablesRaw.split(',').map(s => s.trim()).filter(Boolean) : [];

    const body = { name: name, role: role };
    if (ttl) body.ttl = ttl;
    if (tables.length) body.tables = tables;

    try {
        const res = await fetch('/api/admin/tokens', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });
        if (res.status === 201) {
            const data = await res.json();
            showMintResult('Token "' + name + '" minted (' + role + ').', data.token);
            document.getElementById('mintTokenForm').reset();
            loadAdminTokens();
        } else {
            const text = await res.text();
            showMintResult('Mint failed: ' + (text || res.status), null);
        }
    } catch (err) {
        showMintResult('Mint failed: ' + err.message, null);
    }
});

async function rotateToken(id) {
    if (!confirm('Rotate this token? The current value stops working immediately.')) return;
    try {
        const res = await fetch('/api/admin/tokens/' + encodeURIComponent(id) + '/rotate', { method: 'POST' });
        if (res.ok) {
            const data = await res.json();
            showMintResult('Token rotated. New value:', data.token);
            loadAdminTokens();
        } else {
            showMintResult('Rotate failed: ' + (await res.text() || res.status), null);
        }
    } catch (err) {
        showMintResult('Rotate failed: ' + err.message, null);
    }
}

async function revokeToken(id, name) {
    if (!confirm('Revoke token "' + name + '"? This cannot be undone.')) return;
    try {
        const res = await fetch('/api/admin/tokens/' + encodeURIComponent(id), { method: 'DELETE' });
        if (res.ok) {
            loadAdminTokens();
        } else {
            alert('Revoke failed: ' + (await res.text() || res.status));
        }
    } catch (err) {
        alert('Revoke failed: ' + err.message);
    }
}

async function loadAdminAudit() {
    const wrap = document.getElementById('adminAuditList');
    wrap.innerHTML = '<div class="loading"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';
    try {
        const res = await fetch('/api/admin/audit?limit=100');
        if (!res.ok) {
            wrap.innerHTML = '<div class="empty">Failed to load audit log (' + res.status + ')</div>';
            return;
        }
        const data = await res.json();
        const events = (data && data.events) || [];
        if (events.length === 0) {
            wrap.innerHTML = '<div class="empty">No audit events</div>';
            return;
        }
        let html = '<table class="admin-table audit-table"><thead><tr>' +
            '<th>Time</th><th>Principal</th><th>Outcome</th><th>Action</th><th>SQL</th>' +
            '</tr></thead><tbody>';
        for (const ev of events) {
            const ts = ev.ts ? new Date(ev.ts).toLocaleTimeString() : '';
            const who = (ev.principal || ev.principal_id || '?') + (ev.role ? ' (' + ev.role + ')' : '');
            const outcomeClass = ev.outcome === 'allowed' ? 'ok' : (ev.outcome === 'denied' ? 'denied' : 'err');
            const action = ev.detail || (ev.method + ' ' + ev.path) || '';
            const sql = ev.sql ? escapeHtml(ev.sql) : '<span class="muted">&mdash;</span>';
            html += '<tr><td class="nowrap">' + escapeHtml(ts) + '</td>' +
                '<td>' + escapeHtml(who) + '</td>' +
                '<td><span class="outcome-badge outcome-' + outcomeClass + '">' + escapeHtml(ev.outcome || '') + '</span></td>' +
                '<td>' + escapeHtml(action) + '</td>' +
                '<td class="audit-sql">' + sql + '</td></tr>';
        }
        html += '</tbody></table>';
        wrap.innerHTML = html;
    } catch (e) {
        wrap.innerHTML = '<div class="empty">Error: ' + escapeHtml(e.message) + '</div>';
    }
}

// Close admin modal on backdrop click.
document.getElementById('adminModal').addEventListener('click', function(e) {
    if (e.target === this) closeAdminPanel();
});

// Initialize on load
window.addEventListener('DOMContentLoaded', function() {
    loadTheme();
    loadSchema();
    loadHistory();
    loadSavedQueries();
    renderTabs();
    initAdmin();
});
