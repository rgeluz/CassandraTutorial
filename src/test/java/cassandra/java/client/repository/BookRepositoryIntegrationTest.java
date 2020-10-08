package cassandra.java.client.repository;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import cassandra.java.client.CassandraConnector;
import cassandra.java.client.domain.Book;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.UUIDs;
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

    @Test
    public void whenAddingANewBook_thenBookExists() {
        bookRepository.createTableBooksByTitle();

        String title = "Effective Java";
        String author = "Joshua Bloch";
        Book book = new Book(UUIDs.timeBased(), title, author,"Programming");
        bookRepository.insertbookByTitle(book);

        Book savedBook = bookRepository.selectByTitle(title);
        assertEquals(book.getTitle(), savedBook.getTitle());
    }

    @Test
    public void whenAddingANewBookBatch_ThenBookAddedInAllTables() {
        bookRepository.createTable();

        bookRepository.createTableBooksByTitle();

        String title = "Effective Java";
        String author = "Joshua Bloch";
        Book book = new Book(UUIDs.timeBased(), title, author,"Programming");
        bookRepository.insertBookBatch(book);

        List<Book> books = bookRepository.selectAll();

        assertEquals(1, books.size());
        assertTrue(
                books.stream().anyMatch(
                        b -> b.getTitle().equals("Effective Java")));

        List<Book> booksByTitle = bookRepository.selectAllBookByTitle();

        assertEquals(1, booksByTitle.size());
        assertTrue(
                booksByTitle.stream().anyMatch(
                        b -> b.getTitle().equals("Effective Java")));
    }

    @Test
    public void whenSelectingAll_thenReturnAllRecords() {
        bookRepository.createTable();

        Book book = new Book(
                UUIDs.timeBased(), "Effective Java", "Joshua Bloch","Programming");
        bookRepository.insertbook(book);

        book = new Book(
                UUIDs.timeBased(), "Clean Code", "Robert C. Martin","Programming");
        bookRepository.insertbook(book);

        List<Book> books = bookRepository.selectAll();

        assertEquals(2, books.size());
        assertTrue(books.stream().anyMatch(b -> b.getTitle()
                .equals("Effective Java")));
        assertTrue(books.stream().anyMatch(b -> b.getTitle()
                .equals("Clean Code")));
    }


    @Test(expected = InvalidQueryException.class)
    public void whenDeletingATable_thenUnconfiguredTable() {
        bookRepository.createTable();
        bookRepository.deleteTable("books");

        session.execute("SELECT * FROM " + KEYSPACE_NAME + ".books;");
    }

}
