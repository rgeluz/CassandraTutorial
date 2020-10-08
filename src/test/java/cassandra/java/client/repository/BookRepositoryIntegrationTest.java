package cassandra.java.client.repository;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import cassandra.java.client.CassandraConnector;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class BookRepositoryIntegrationTest {

    private BookRepository bookRepository;
    private Session session;

    final String KEYSPACE_NAME = "testLibrary";
    final String BOOKS = "books";
    final String BOOKS_BY_TITLE = "booksByTitle";

    @Before
    public void connect() {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9142);
        this.session = client.getSession();
        bookRepository = new BookRepository(session);
    }
    @Test
    public void whenCreatingATable_thenCreatedCorrectly() {
        bookRepository.createTable();

        ResultSet result = session.execute(
                "SELECT * FROM " + KEYSPACE_NAME + ".books;");

        List<String> columnNames =
                result.getColumnDefinitions().asList().stream()
                        .map(cl -> cl.getName())
                        .collect(Collectors.toList());

        assertEquals(columnNames.size(), 3);
        assertTrue(columnNames.contains("id"));
        assertTrue(columnNames.contains("title"));
        assertTrue(columnNames.contains("subject"));
    }


    @Test
    public void whenAlteringTable_thenAddedColumnExists() {
        bookRepository.createTable();

        bookRepository.alterTablebooks("publisher", "text");

        ResultSet result = session.execute(
                "SELECT * FROM " + KEYSPACE_NAME + "." + "books" + ";");

        boolean columnExists = result.getColumnDefinitions().asList().stream()
                .anyMatch(cl -> cl.getName().equals("publisher"));

        assertTrue(columnExists);
    }




}
