import com.sun.deploy.util.ArrayUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Daniel on 2016-12-07.
 */
public class SQLquery {
    public Connection connection = openConnection();
    String query = "INSERT INTO TABLEONE (" +
            "id," +
            "parent_id," +
            "link_id," +
            "name ," +
            "author," +
            "body," +
            "subreddit_id," +
            "subreddit," +
            "score," +
            "created_utc)" +
            "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public SQLquery() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    }

    public void indexDB() throws SQLException {
        String query = "CREATE UNIQUE INDEX index_name ON TABLEONE ( author, link_id)";
        executeStatement(query);
    }

    public int commentsPostedByUser(String userName) throws SQLException {
        String query = "SELECT COUNT(*) FROM TABLEONE WHERE author ='" + userName + "'";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery(query);
        rs.next();
        return rs.getInt(1);
    }

    public int subRedditCommentsPerDay(String name, int date) throws SQLException {
        String query = "SELECT COUNT(*) FROM TABLEONE WHERE subreddit_id ='" + name + "'" +
                "AND date_format(from_unixtime(created_utc),'%m-%d-%Y') " +
                "= date_format(from_unixtime("+ date +"),'%m-%d-%Y')";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery(query);
        rs.next();
        return rs.getInt(1);
    }

    public int commentsWithLol() throws SQLException {
        String query = "SELECT COUNT(*) FROM TABLEONE WHERE body LIKE '%lol%'";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery(query);
        rs.next();
        return rs.getInt(1);
    }

    public ArrayList usersOnLinkAlsoOnWhichSubreddits(String linkID) throws SQLException {
        String query = "SELECT DISTINCT subreddit FROM TABLEONE WHERE " +
                "author in (SELECT author FROM TABLEONE WHERE " +
                "link_id= '" + linkID + "') ORDER by author";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery(query);
        ArrayList<String> resList = new ArrayList<String>();
        while (rs.next()) {
            resList.add(rs.getString(1));
        }
        return resList;
    }

    public String[] subredditHighestLowestScoreComments() throws SQLException {
        String query = "select subreddit, max(score) " +
                "as score from TABLEONE union select " +
                "subreddit,min(score) as score from TABLEONE";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery(query);
    }

    public ArrayList<String> topBottomCombinedScore() throws SQLException {
        String query = "SELECT max(sums) as s " +
                "from (SELECT author,SUM(score) " +
                "as sums FROM TABLEONE group by author) as subquery " +
                "union " +
                "select min(sums) as s " +
                "from (SELECT author,SUM(score) " +
                "as sums FROM TABLEONE group by author) as subquery";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        rs.next();
        System.out.println(rs.getString(1) + rs.getString(2));
        rs.next();
        System.out.println(rs.getString(1) + rs.getString(2));

        ArrayList<String> arrayList = new ArrayList<String>();
        //arrayList.addAll(Arrays.asList(r));
        //arrayList.addAll(Arrays.asList(r2));
        return arrayList;
    }

    public ArrayList possibleInteractions(String name) throws SQLException {
        String query = "SELECT DISTINCT author from Reddit" +
                        " where link_id in(" +
                        "select link_id from Reddit " +
                        "where author= '" + name +"')";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
    }

    public ArrayList onlyOneSubreddit() throws SQLException {
        String query = "SELECT author, subreddit FROM (" +
                "SELECT DISTINCT author, subreddit FROM Reddit) " +
                "GROUP BY author HAVING COUNT(*)=1";

        PreparedStatement ps = connection.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
    }


    public void importFileToDB() {
        String query = "LOAD DATA LOCAL INFILE 'RC_2007-10' " +
                "INTO TABLE TABLEONE" +
                " FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"'" +
                " LINES TERMINATED BY \'\\n\'";

        executeStatement(query);


    }

    public void index() {
        String query1 = "ALTER TABLE TABLEONE ADD INDEX (author)";
        String query2 = "ALTER TABLE TABLEONE ADD INDEX (subreddit)";

        executeStatement(query1);
        executeStatement(query2);
    }
    public void addDbEntry(String[] list) {
        //StackTraceElement[] st = Thread.currentThread().getStackTrace();
        //System.out.println(  "create connection called from " + st[2] );

        //Connection connection;

        try {
            //connection = openConnection();

            PreparedStatement ps = connection.prepareStatement(query);
            ps.setString(1, list[0]);
            ps.setString(2, list[1]);
            ps.setString(3, list[2]);
            ps.setString(4, list[3]);
            ps.setString(5, list[4]);
            ps.setString(6, list[5]);
            ps.setString(7, list[6]);
            ps.setString(8, list[7]);
            ps.setString(9, list[8]);
            ps.setInt(10, Integer.valueOf(list[9]));

            ps.executeUpdate();
            //ps.addBatch();
            ps.close();
            //connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropTable(int tableID) {

        String query1 = "DROP TABLE TABLEONE";
        if (tableID == 1) {
            executeStatement(query1);
        }
    }

    public void createMainTable() {
        String query = "CREATE TABLE TABLEONE (" +
                "id VARCHAR(15) NOT NULL, " +
                "parent_id VARCHAR(20) NOT NULL, " +
                "link_id VARCHAR(20) NOT NULL, " +
                "name VARCHAR(100) NOT NULL, " +
                "author VARCHAR(50) NOT NULL, " +
                "body VARCHAR(10000) NOT NULL, " +
                "subreddit_id VARCHAR(15) NOT NULL, " +
                "subreddit VARCHAR(100) NOT NULL, " +
                "score VARCHAR(100) NOT NULL, " +
                "created_utc int(255) NOT NULL, " +
                "PRIMARY KEY (id))";
        executeStatement(query);
    }
    private void executeStatement(String query) {
        try {
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();
            ps.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public Connection openConnection() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        String userName = "root";
        String password = "";
        String url = "jdbc:mysql://localhost:3306/redditdbone";

        DriverManager.setLoginTimeout(5);
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection conn = DriverManager.getConnection(url, userName, password);
        conn.setAutoCommit(false);
        return conn;
    }
}
