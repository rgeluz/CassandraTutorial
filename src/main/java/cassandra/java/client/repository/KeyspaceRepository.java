package cassandra.java.client.repository;

import com.datastax.driver.core.Session;

public class KeyspaceRepository {

    private Session session;

    public KeyspaceRepository(Session session) {
        this.session = session;
    }

    /**
     * Create any keyspace - schema
     * @param keyspaceName              - the name of the schema
     * @param replicationStrategy       - the replication strategy
     * @param replicationFactor         - the number of replicas
     */
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

    public void deleteKeyspace(String keyspaceName) {
        StringBuilder sb =
                new StringBuilder("DROP KEYSPACE ").append(keyspaceName);

        String query = sb.toString();
        session.execute(query);
    }
}
