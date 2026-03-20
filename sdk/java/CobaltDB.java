package com.cobaltdb.sdk;

import java.sql.*;
import java.util.Properties;

/**
 * CobaltDB Java SDK
 *
 * Connect to CobaltDB server using the MySQL JDBC driver.
 * CobaltDB speaks the MySQL wire protocol, so standard MySQL JDBC works out of the box.
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * Connection conn = CobaltDB.connect("127.0.0.1", 3307, "admin", "");
 *
 * // Create table
 * conn.createStatement().execute(
 *     "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
 * );
 *
 * // Insert with prepared statement
 * PreparedStatement ps = conn.prepareStatement("INSERT INTO users VALUES (?, ?)");
 * ps.setInt(1, 1);
 * ps.setString(2, "Alice");
 * ps.executeUpdate();
 *
 * // Query
 * ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users");
 * while (rs.next()) {
 *     System.out.println(rs.getInt("id") + ": " + rs.getString("name"));
 * }
 *
 * conn.close();
 * }</pre>
 *
 * <h2>Requirements:</h2>
 * <pre>
 * Maven: mysql:mysql-connector-java:8.0.33
 * Gradle: implementation 'mysql:mysql-connector-java:8.0.33'
 * </pre>
 */
public class CobaltDB {

    public static final String VERSION = "0.3.0";
    public static final int DEFAULT_PORT = 3307;
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final String DEFAULT_USER = "admin";

    /**
     * Connect to a CobaltDB server.
     *
     * @param host Server hostname
     * @param port MySQL protocol port (default: 3307)
     * @param user Username
     * @param password Password
     * @return JDBC Connection
     * @throws SQLException if connection fails
     */
    public static Connection connect(String host, int port, String user, String password)
            throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/?allowPublicKeyRetrieval=true&useSSL=false",
                host, port);
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        return DriverManager.getConnection(url, props);
    }

    /**
     * Connect with default settings.
     */
    public static Connection connect() throws SQLException {
        return connect(DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER, "");
    }

    /**
     * Connect with host and port only.
     */
    public static Connection connect(String host, int port) throws SQLException {
        return connect(host, port, DEFAULT_USER, "");
    }

    /**
     * Execute a query and print results (utility for testing).
     */
    public static void printQuery(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();

            // Print headers
            for (int i = 1; i <= cols; i++) {
                System.out.printf("%-20s", meta.getColumnName(i));
            }
            System.out.println();

            // Print rows
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    System.out.printf("%-20s", rs.getString(i));
                }
                System.out.println();
            }
        }
    }

    /**
     * Example main method demonstrating CobaltDB Java SDK usage.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("CobaltDB Java SDK v" + VERSION);
        System.out.println("Connecting to CobaltDB server...");

        try (Connection conn = connect()) {
            System.out.println("Connected successfully!");

            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS demo (id INTEGER PRIMARY KEY, name TEXT)"
            );
            conn.createStatement().execute("INSERT INTO demo VALUES (1, 'Hello from Java')");

            printQuery(conn, "SELECT * FROM demo");
        } catch (SQLException e) {
            System.err.println("Connection failed: " + e.getMessage());
            System.err.println("Make sure CobaltDB server is running on port " + DEFAULT_PORT);
        }
    }
}
