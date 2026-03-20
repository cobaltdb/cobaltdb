# CobaltDB Java SDK

Connect to CobaltDB server using the standard MySQL JDBC driver.

## Dependencies

### Maven
```xml
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>8.0.33</version>
</dependency>
```

### Gradle
```groovy
implementation 'mysql:mysql-connector-java:8.0.33'
```

## Usage

```java
import com.cobaltdb.sdk.CobaltDB;
import java.sql.*;

// Connect to CobaltDB
Connection conn = CobaltDB.connect("127.0.0.1", 3307, "admin", "");

// Create table
conn.createStatement().execute(
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
);

// Prepared statements
PreparedStatement ps = conn.prepareStatement("INSERT INTO users VALUES (?, ?, ?)");
ps.setInt(1, 1);
ps.setString(2, "Alice");
ps.setString(3, "alice@example.com");
ps.executeUpdate();

// Query
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users");
while (rs.next()) {
    System.out.println(rs.getInt("id") + ": " + rs.getString("name"));
}

conn.close();
```

## With Try-With-Resources

```java
try (Connection conn = CobaltDB.connect()) {
    try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE id = ?")) {
        ps.setInt(1, 1);
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                System.out.println(rs.getString("name"));
            }
        }
    }
}
```

## Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Server hostname |
| `port` | `3307` | MySQL protocol port |
| `user` | `admin` | Username |
| `password` | `""` | Password |
