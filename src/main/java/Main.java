import com.sun.org.apache.xpath.internal.SourceTree;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Daniel on 2016-12-07.
 */
public class Main {

    private static SQLquery sqLquery;
    private static JsonHandler jsonHandler;
    private static String file = "RC_2007-10";
    private static String userNameToFind = "gigaquack";
    private static String subredditIdToFind = "t5_6";
    private static String linkIdToFind = "t3_5yba3";
    private static int date = 1192450635;

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, FileNotFoundException {
        System.out.println("Starting application");
        establishConnection();
/*
        resetDB();
        progressCounter(150429);

        long startTime = System.nanoTime();
        populateDB();
        long endTime = System.nanoTime();
        sqLquery.index();
*/
        executeQueries();
        closeConnection();
/*

        System.out.println("Database Populated in: ");
        System.out.println((endTime - startTime) + " Nanoseconds");
        System.out.println((endTime - startTime) /1000000 + " Milliseconds");
        System.out.println((endTime - startTime) /1000000000 + "Seconds");
*/
    }
    private static void executeQueries() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        sqLquery = new SQLquery();

        System.out.println("1. Number of comments posted by '" + userNameToFind + "': " + sqLquery.commentsPostedByUser(userNameToFind));
        System.out.println("2. Number of comments posted to subreddit '" + subredditIdToFind + "': " + sqLquery.subRedditCommentsPerDay(subredditIdToFind, date));
        System.out.println("3. Number of comments that include the word 'lol': " + sqLquery.commentsWithLol());
        System.out.println("4. Subreddits also commented on by users on subreddit: " + linkIdToFind);
        System.out.println("5. ");
        for (Object o : sqLquery.usersOnLinkAlsoOnWhichSubreddits(linkIdToFind)) {
            System.out.println(o.toString());
        }
        ArrayList<String> res = sqLquery.topBottomCombinedScore();
        System.out.println("6. Highest score by: " + res.get(0) + ": " +res.get(1));
        System.out.println("6. Lowest score by: " + res.get(2) + ": " +res.get(3));
    }
    private static long numberOfLines() throws FileNotFoundException {
        System.out.println("Counting objects to store...");
        long lines = 0;
        Scanner input = new Scanner(new File(file));

        while (input.hasNextLine()) {
            lines++;
        }
        return lines;
    }

    private static void progressCounter(final long objects) {
        final Thread thread = new Thread() {
            public void run() {
                while ((long)jsonHandler.n < objects) {
                    int percentage = (int) (jsonHandler.n * 100.0 / objects + 0.5);
                    System.out.print("\r" + jsonHandler.n + " / " + objects + " |" + percentage + "%");
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
    }

    private static void establishConnection() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        jsonHandler = new JsonHandler();
        System.out.println("Attempting to connect to MySQL Database.");
        try {
            sqLquery = new SQLquery();
            sqLquery.connection = sqLquery.openConnection();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Connection establishing error.");
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        System.out.println("Connection established!");
    }

    private static void closeConnection() {
        System.out.println("Closing connection.");
        try {
            sqLquery.connection.close();
        } catch (SQLException e) {
            System.out.println("Connection close error.");
            e.printStackTrace();
        }
    }

    private static void populateDB() {
        System.out.println("Starting database population.");
        jsonHandler.parseJson(file);
        System.out.println();
    }

    private static void dropTable(int id) {
        System.out.println("Starting Clear.");
        sqLquery.dropTable(id);
        System.out.println("Clear Finished!");
    }

    private static void buildTable() throws SQLException {
        System.out.println("Building table");
        sqLquery.createMainTable();
        System.out.println("Table built!");
    }

    private static void resetDB() throws SQLException {
        System.out.println();
        System.out.println("Resetting database tables.");
        dropTable(1);
        buildTable();

    }
}
