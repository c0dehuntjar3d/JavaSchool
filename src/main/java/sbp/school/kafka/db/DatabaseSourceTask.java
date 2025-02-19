package sbp.school.kafka.db;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class DatabaseSourceTask extends SourceTask {
    private Connection connection;
    private String table;
    private String topic;
    private Integer offset;
    private Integer limit;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            String hostname = props.get(DatabaseSourceConnectorConfig.HOSTNAME_CONFIG);
            int port = Integer.parseInt(props.get(DatabaseSourceConnectorConfig.PORT_CONFIG));
            String database = props.get(DatabaseSourceConnectorConfig.DATABASE_CONFIG);
            String username = props.get(DatabaseSourceConnectorConfig.USERNAME_CONFIG);
            String password = props.get(DatabaseSourceConnectorConfig.PASSWORD_CONFIG);
            table = props.get(DatabaseSourceConnectorConfig.TABLE_CONFIG);
            topic = props.get(DatabaseSourceConnectorConfig.TOPIC_CONFIG);
            offset = Integer.parseInt(props.get(DatabaseSourceConnectorConfig.OFFSET));
            limit = Integer.parseInt(props.get(DatabaseSourceConnectorConfig.LIMIT));

            String url = String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
            connection = DriverManager.getConnection(url, username, password);
            log.info("Prepared connection: {}", connection.getClientInfo());
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to PostgreSQL", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        String query = "SELECT * FROM " + table + " WHERE id > ? LIMIT ?";

        try (Statement stmt = connection.createStatement()) {

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setLong(1, offset);
            pstmt.setLong(2, limit);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                records.add(record(rs));
                offset++;
            }
        } catch (Exception e) {
            log.error("error for query: {}", query, e);
        }
        return records;
    }

    private SourceRecord record(ResultSet rs) throws SQLException {
        int id = rs.getInt("id");
        String account = rs.getString("account");
        double value = rs.getDouble("value");
        String date = rs.getString("date");

        SourceRecord record = new SourceRecord(
                null,
                null,
                topic,
                Schema.STRING_SCHEMA,
                String.format("id: %d, account: %s, value: %.2f, date: %s", id, account, value, date));
        return record;
    }

    @Override
    public void stop() {
        try {
            if (connection != null)
                connection.close();
        } catch (Exception ignored) {
        }
    }
}
