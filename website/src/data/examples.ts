export interface Example {
  id: string
  title: string
  description: string
  code: string
  tags: string[]
}

export const examplesData: Record<string, Example[]> = {
  basic: [
    {
      id: 'create-table',
      title: 'Creating Tables',
      description: 'Basic table creation with constraints and indexes.',
      tags: ['DDL', 'Schema'],
      code: `-- Create a users table with constraints
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    full_name TEXT,
    age INTEGER CHECK (age >= 0),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create an index for faster lookups
CREATE INDEX idx_users_email ON users(email);

-- Insert sample data
INSERT INTO users (username, email, full_name, age) VALUES
    ('john_doe', 'john@example.com', 'John Doe', 30),
    ('jane_smith', 'jane@example.com', 'Jane Smith', 25);`,
    },
    {
      id: 'crud-operations',
      title: 'CRUD Operations',
      description: 'Create, Read, Update, and Delete operations.',
      tags: ['DML', 'Basic'],
      code: `-- CREATE: Insert new records
INSERT INTO users (username, email, full_name, age)
VALUES ('bob_wilson', 'bob@example.com', 'Bob Wilson', 35);

-- READ: Query data
SELECT * FROM users WHERE is_active = TRUE;

-- READ: With conditions and ordering
SELECT full_name, email
FROM users
WHERE age > 25
ORDER BY created_at DESC
LIMIT 10;

-- UPDATE: Modify existing records
UPDATE users
SET full_name = 'Johnathan Doe', age = 31
WHERE username = 'john_doe';

-- DELETE: Remove records
DELETE FROM users WHERE is_active = FALSE;`,
    },
    {
      id: 'joins',
      title: 'Joins',
      description: 'Combining data from multiple tables.',
      tags: ['Query', 'Join'],
      code: `-- Create orders table
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    total DECIMAL(10, 2),
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- INNER JOIN: Get users with their orders
SELECT
    u.full_name,
    u.email,
    o.id as order_id,
    o.total,
    o.status
FROM users u
INNER JOIN orders o ON u.id = o.user_id
WHERE o.status = 'completed';

-- LEFT JOIN: All users, even without orders
SELECT
    u.full_name,
    COUNT(o.id) as order_count,
    COALESCE(SUM(o.total), 0) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id;`,
    },
  ],
  advanced: [
    {
      id: 'window-functions',
      title: 'Window Functions',
      description: 'Advanced analytics with window functions.',
      tags: ['Analytics', 'Window'],
      code: `-- Running totals
SELECT
    date,
    amount,
    SUM(amount) OVER (ORDER BY date) as running_total
FROM sales
ORDER BY date;

-- Ranking
SELECT
    employee_name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    RANK() OVER (ORDER BY salary DESC) as overall_rank
FROM employees;

-- Moving averages
SELECT
    date,
    revenue,
    AVG(revenue) OVER (
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as week_moving_avg
FROM daily_revenue;`,
    },
    {
      id: 'ctes',
      title: 'Common Table Expressions',
      description: 'Using CTEs for complex queries.',
      tags: ['CTE', 'Advanced'],
      code: `-- Non-recursive CTE
WITH top_customers AS (
    SELECT
        customer_id,
        SUM(order_total) as total_spent
    FROM orders
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 10
)
SELECT
    c.name,
    tc.total_spent
FROM customers c
JOIN top_customers tc ON c.id = tc.customer_id;

-- Recursive CTE: Organization hierarchy
WITH RECURSIVE org_tree AS (
    -- Base case: top-level managers
    SELECT id, name, manager_id, 0 as level
    FROM employees
    WHERE manager_id IS NULL

    UNION ALL

    -- Recursive case
    SELECT e.id, e.name, e.manager_id, ot.level + 1
    FROM employees e
    JOIN org_tree ot ON e.manager_id = ot.id
)
SELECT
    REPEAT('  ', level) || name as org_chart,
    level
FROM org_tree
ORDER BY level, name;`,
    },
    {
      id: 'json-operations',
      title: 'JSON Operations',
      description: 'Working with JSON data.',
      tags: ['JSON', 'NoSQL'],
      code: `-- Store JSON data
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    event_type TEXT,
    payload JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert JSON
INSERT INTO events (event_type, payload) VALUES
    ('user_signup', '{"user_id": 123, "email": "user@example.com", "plan": "pro"}'),
    ('purchase', '{"order_id": 456, "amount": 99.99, "items": [1, 2, 3]}');

-- Query JSON with extraction
SELECT
    event_type,
    JSON_EXTRACT(payload, '$.user_id') as user_id,
    JSON_EXTRACT(payload, '$.email') as email
FROM events
WHERE event_type = 'user_signup';

-- Update JSON fields
UPDATE events
SET payload = JSON_SET(payload, '$.processed', TRUE)
WHERE event_type = 'purchase';`,
    },
    {
      id: 'full-text-search',
      title: 'Full-Text Search',
      description: 'Implementing search functionality.',
      tags: ['FTS', 'Search'],
      code: `-- Create FTS virtual table
CREATE VIRTUAL TABLE articles_fts USING fts4(
    title,
    content,
    author,
    tokenize=porter
);

-- Insert documents
INSERT INTO articles_fts (title, content, author) VALUES
    ('Getting Started with CobaltDB',
     'CobaltDB is a fast, embeddable SQL database...',
     'John Doe'),
    ('Advanced SQL Queries',
     'Learn about window functions and CTEs...',
     'Jane Smith');

-- Search for documents
SELECT title, snippet(articles_fts) as snippet
FROM articles_fts
WHERE articles_fts MATCH 'database SQL';

-- Prefix search
SELECT * FROM articles_fts WHERE articles_fts MATCH 'adv*';

-- Phrase search
SELECT * FROM articles_fts WHERE articles_fts MATCH '"window functions"';`,
    },
  ],
  realworld: [
    {
      id: 'ecommerce-schema',
      title: 'E-Commerce Schema',
      description: 'Complete schema for an e-commerce application.',
      tags: ['Schema', 'E-commerce'],
      code: `-- Users
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    full_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    sku TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    category_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',
    total_amount DECIMAL(10, 2) NOT NULL,
    shipping_address JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Order Items
CREATE TABLE order_items (
    id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Sales report query
SELECT
    DATE(o.created_at) as sale_date,
    COUNT(DISTINCT o.id) as order_count,
    SUM(oi.quantity) as items_sold,
    SUM(o.total_amount) as revenue
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
WHERE o.status = 'completed'
GROUP BY DATE(o.created_at)
ORDER BY sale_date DESC;`,
    },
    {
      id: 'blog-system',
      title: 'Blog System with Comments',
      description: 'Blog schema with nested comments.',
      tags: ['Blog', 'Recursive'],
      code: `-- Blog posts
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    published BOOLEAN DEFAULT FALSE,
    published_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tags
CREATE TABLE tags (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE post_tags (
    post_id INTEGER,
    tag_id INTEGER,
    PRIMARY KEY (post_id, tag_id)
);

-- Nested comments
CREATE TABLE comments (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    parent_id INTEGER,
    author_name TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (post_id) REFERENCES posts(id),
    FOREIGN KEY (parent_id) REFERENCES comments(id)
);

-- Get all comments for a post with nesting
WITH RECURSIVE comment_tree AS (
    SELECT
        id,
        parent_id,
        author_name,
        content,
        created_at,
        0 as depth,
        CAST(id AS TEXT) as path
    FROM comments
    WHERE post_id = 1 AND parent_id IS NULL

    UNION ALL

    SELECT
        c.id,
        c.parent_id,
        c.author_name,
        c.content,
        c.created_at,
        ct.depth + 1,
        ct.path || '/' || c.id
    FROM comments c
    JOIN comment_tree ct ON c.parent_id = ct.id
    WHERE c.post_id = 1
)
SELECT
    REPEAT('  ', depth) || author_name as indented_author,
    content,
    created_at
FROM comment_tree
ORDER BY path;`,
    },
    {
      id: 'user-analytics',
      title: 'User Analytics Dashboard',
      description: 'Queries for analytics and metrics.',
      tags: ['Analytics', 'Metrics'],
      code: `-- Daily active users
SELECT
    DATE(login_time) as date,
    COUNT(DISTINCT user_id) as dau
FROM user_sessions
GROUP BY DATE(login_time)
ORDER BY date DESC;

-- User retention (cohort analysis)
WITH user_cohorts AS (
    SELECT
        user_id,
        DATE(MIN(created_at)) as cohort_date
    FROM user_sessions
    GROUP BY user_id
),
retention AS (
    SELECT
        uc.cohort_date,
        CAST((JULIANDAY(us.login_time) - JULIANDAY(uc.cohort_date)) / 7 AS INTEGER) as week_num,
        COUNT(DISTINCT us.user_id) as retained_users
    FROM user_cohorts uc
    JOIN user_sessions us ON uc.user_id = us.user_id
    GROUP BY uc.cohort_date, week_num
)
SELECT
    cohort_date,
    MAX(CASE WHEN week_num = 0 THEN retained_users END) as week_0,
    MAX(CASE WHEN week_num = 1 THEN retained_users END) as week_1,
    MAX(CASE WHEN week_num = 4 THEN retained_users END) as week_4
FROM retention
GROUP BY cohort_date
ORDER BY cohort_date DESC;

-- Funnel analysis
WITH funnel AS (
    SELECT
        COUNT(DISTINCT CASE WHEN event = 'page_view' THEN user_id END) as step_1,
        COUNT(DISTINCT CASE WHEN event = 'add_to_cart' THEN user_id END) as step_2,
        COUNT(DISTINCT CASE WHEN event = 'checkout_start' THEN user_id END) as step_3,
        COUNT(DISTINCT CASE WHEN event = 'purchase_complete' THEN user_id END) as step_4
    FROM events
    WHERE created_at >= DATE('now', '-30 days')
)
SELECT
    step_1 as visitors,
    step_2 as added_to_cart,
    ROUND(100.0 * step_2 / step_1, 2) as cart_rate,
    step_3 as started_checkout,
    ROUND(100.0 * step_3 / step_2, 2) as checkout_rate,
    step_4 as purchased,
    ROUND(100.0 * step_4 / step_1, 2) as conversion_rate
FROM funnel;`,
    },
  ],
  patterns: [
    {
      id: 'soft-delete',
      title: 'Soft Delete Pattern',
      description: 'Implementing soft deletes with triggers.',
      tags: ['Pattern', 'Triggers'],
      code: `-- Add deleted_at column
ALTER TABLE users ADD COLUMN deleted_at TIMESTAMP;

-- Create trigger for soft delete
CREATE TRIGGER soft_delete_user
INSTEAD OF DELETE ON users
FOR EACH ROW
BEGIN
    UPDATE users SET deleted_at = datetime('now')
    WHERE id = OLD.id;
END;

-- View for active records only
CREATE VIEW active_users AS
SELECT * FROM users WHERE deleted_at IS NULL;

-- Query active users (automatically filters deleted)
SELECT * FROM active_users;

-- Include soft-deleted in admin queries
SELECT * FROM users WHERE deleted_at IS NOT NULL;`,
    },
    {
      id: 'audit-log',
      title: 'Audit Logging',
      description: 'Tracking changes with triggers.',
      tags: ['Pattern', 'Audit'],
      code: `-- Audit log table
CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    action TEXT NOT NULL,  -- INSERT, UPDATE, DELETE
    old_data JSON,
    new_data JSON,
    changed_by TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trigger for INSERT
CREATE TRIGGER audit_insert_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, new_data)
    VALUES ('users', NEW.id, 'INSERT', json_object(
        'username', NEW.username,
        'email', NEW.email,
        'full_name', NEW.full_name
    ));
END;

-- Trigger for UPDATE
CREATE TRIGGER audit_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO audit_log (table_name, record_id, action, old_data, new_data)
    VALUES ('users', NEW.id, 'UPDATE', json_object(
        'username', OLD.username,
        'email', OLD.email,
        'full_name', OLD.full_name
    ), json_object(
        'username', NEW.username,
        'email', NEW.email,
        'full_name', NEW.full_name
    ));
END;

-- Query audit trail for a record
SELECT * FROM audit_log
WHERE table_name = 'users' AND record_id = 1
ORDER BY changed_at DESC;`,
    },
    {
      id: 'pagination',
      title: 'Cursor-Based Pagination',
      description: 'Efficient pagination for large datasets.',
      tags: ['Pattern', 'Performance'],
      code: `-- Offset pagination (simple but slow on large tables)
SELECT * FROM posts
ORDER BY created_at DESC
LIMIT 20 OFFSET 1000;

-- Cursor-based pagination (much faster)
-- First page
SELECT * FROM posts
ORDER BY created_at DESC, id DESC
LIMIT 21;  -- Get 21 to check if there's a next page

-- Next page (using last row from previous query)
SELECT * FROM posts
WHERE (created_at, id) < ('2024-01-15 10:30:00', 12345)
ORDER BY created_at DESC, id DESC
LIMIT 21;

-- With page info
WITH paginated AS (
    SELECT *,
        LAG(id) OVER (ORDER BY created_at DESC, id DESC) as prev_id,
        LEAD(id) OVER (ORDER BY created_at DESC, id DESC) as next_id
    FROM posts
    WHERE (created_at, id) < ('2024-01-15 10:30:00', 12345)
    ORDER BY created_at DESC, id DESC
    LIMIT 20
)
SELECT * FROM paginated;`,
    },
  ],
}
