package cassandra.java.client.repository;

import com.datastax.driver.core.Session;

public class KeyspaceRepository {

    private Session session;

    public void createKeyspace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS")
                    .append(keyspaceName).append(" WITH replication = {")
                    .append("'class':").append(replicationStrategy)
                    .append("','replication_factor':").append(replicationFactor)
                    .append("};");

        String query = sb.toString();
        session.execute(query);
    }


}
