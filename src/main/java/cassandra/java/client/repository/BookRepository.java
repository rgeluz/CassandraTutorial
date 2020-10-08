package cassandra.java.client.repository;

import cassandra.java.client.domain.Book;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class BookRepository {
    private static final String TABLE_NAME = "books";
    private static final String TABLE_NAME_BY_TITLE = TABLE_NAME + "ByTitle";
    private Session session;

    public BookRepository(Session session) {
        this.session = session;
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);
    }

    public void createTableBooksByTitle() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append("booksByTitle").append("(")
                .append("id uuid, ")
                .append("title text,")
                .append("PRIMARY KEY (title, id));");

        String query = sb.toString();
        session.execute(query);
    }


    public void alterTablebooks(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(TABLE_NAME).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertbook(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append("(id, title, author, subject) ")
                .append("VALUES (").append(book.getId()).append(", '")
                .append(book.getTitle()).append("', '")
                .append(book.getAuthor()).append("', '")
                .append(book.getSubject()).append("');");

        final String query = sb.toString();
        session.execute(query);
    }

    public void insertbookByTitle(Book book) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME_BY_TITLE).append("(id, title) ")
                .append("VALUES (").append(book.getId())
                .append(", '").append(book.getTitle()).append("');");

        String query = sb.toString();
        session.execute(query);
    }


    public Book selectByTitle(String title) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME_BY_TITLE)
                .append(" WHERE title = '")
                .append(title).append("';");

        final String query = sb.toString();

        ResultSet rs = session.execute(query);

        List<Book> books = new ArrayList<Book>();

        for (Row r : rs) {
            Book s = new Book(r.getUUID("id"), r.getString("title"), null, null);
            books.add(s);
        }

        return books.get(0);
    }

    public List<Book> selectAll() {
        StringBuilder sb =
                new StringBuilder("SELECT * FROM ").append(TABLE_NAME);

        String query = sb.toString();
        ResultSet rs = session.execute(query);

        List<Book> books = new ArrayList<Book>();

        rs.forEach(r -> {
            books.add(new Book(
                    r.getUUID("id"),
                    r.getString("title"),
                    r.getString("author"),
                    r.getString("subject")));
        });
        return books;
    }




}
