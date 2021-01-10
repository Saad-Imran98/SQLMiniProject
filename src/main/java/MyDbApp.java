
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Scanner;
import java.io.*;

public class MyDbApp {

    public final static String DELAYEDFLIGHTSFILE = "/delayedFlights";
    public final static String AIRPORTFILE = "/airport";

    public static Connection connectToDatabase(String user, String password, String database) {
        System.out.println("---Connecting to PSQL---");

        Connection connection = null;
        try {
            connection =
                    DriverManager.getConnection("jdbc:postgresql://localhost/CS2855/zhac077", user, password);
        } catch (SQLException e) {
            System.out.println("Connection failed: ");
            e.printStackTrace();
        }
        return connection;
    }

    public static ResultSet executeSelect(Connection connection, String query) {
        Statement st = null;
        ResultSet rs = null;
        try {
            st = connection.createStatement();
            rs = st.executeQuery(query);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return rs;
    }

    /**
     * EXECUTE CREATE TABLE
     * @param connection
     * @param table
     */
    public static void executeCreate(Connection connection, String table) {
        Statement st = null;
        try {
            st = connection.createStatement();
            st.execute("CREATE TABLE "+table);
            System.out.println("Created Table: " + table);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * EXECUTE DROP TABLE
     * @param connection
     * @param table
     */
    public static void executeDrop(Connection connection, String table) {
        Statement st = null;
        try {
            st = connection.createStatement();
            st.execute("DROP TABLE "+table);
            System.out.println("Dropped Table: "+ table);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void executeDropIfExists(Connection connection, String table) {
        Statement st = null;
        try {
            st = connection.createStatement();
            st.execute("DROP TABLE IF EXISTS "+table);
            System.out.println("Dropped Table: "+ table);
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    static boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        try {
            double d = Double.parseDouble(strNum);
        } catch (NumberFormatException nfe) {
            return false;
        }
        return true;
    }

    static void insertFromFile(String filePath, PreparedStatement statement) throws IOException {
        File file = new File(filePath);
        Scanner scanner = new Scanner(file, "ISO-8859-1");
        int batchCount = 0;
        while(scanner.hasNext()){
            String line = scanner.nextLine();
            String[] split = line.split(",");
            if (split.length <4) {
                System.out.println(line);
                System.out.println(scanner.hasNext());
            }
            try {
                for (int i = 0; i< split.length; i++) {
                    if (isNumeric(split[i])) {
                        statement.setInt(i+1, Integer.parseInt(split[i]));
                    } else {
                        statement.setString(i+1, split[i]);
                    }
                }
                batchCount++;
                statement.addBatch();
                if (batchCount % 100 ==0 || !scanner.hasNext()){
                    statement.executeBatch();
                }
                statement.clearParameters();

            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        scanner.close();
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String line = "";
            for (int i = 0; i<2; i++){
                line+= rs.getString(i+1) + " ";
            }
            System.out.println(line);
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter Username: ");

        String user = scanner.nextLine();
        System.out.println("Enter Password: ");

        String password = scanner.nextLine();

        String database = "teachdb.cs.rhul.ac.uk";
        Connection connection = connectToDatabase(user, password, database);

        if (connection != null) {
            System.out.println("Connected to database! : " + database);
        } else {
            System.out.println("Failed to connect");
            return;
        }

        //CREATING TABLES QUERIES
        String airportsQuery = "airport(airportCode varchar(3) primary key," +
                "airportName varchar(75)," +
                "City varchar(75)," +
                "State varchar(2)" +
                ")";
        String delayedFlightsQuery = "delayedFlights(Flight_ID int primary key" +
                ", Month int, " +
                "DayofMonth int, " +
                "DayOfWeek int, " +
                "DepTime int, " +
                "ScheduledDepTime int, " +
                "ArrTime int, " +
                "ScheduledArrTime int, " +
                "UniqueCarrier varchar(4), " +
                "FlightNum int, " +
                "ActualFlightTime int, " +
                "scheduledFlightTime int, " +
                "AirTime int, " +
                "ArrDelay int, " +
                "DepDelay int, " +
                "Orig varchar(3) REFERENCES airport(airportCode), " +
                "Dest varchar(3) REFERENCES airport(airportCode), " +
                "Distance int" +
                ")";

        // DROP AND CREATE TABLES
        executeDropIfExists(connection, "delayedFlights");
        executeDropIfExists(connection, "airport");
        executeCreate(connection, airportsQuery);
        executeCreate(connection, delayedFlightsQuery);

        //INSERT TABLES FROM FILE
        try {
            String insertFlightsStatement = "INSERT INTO delayedflights(Flight_ID, Month, DayofMonth, DayOfWeek, DepTime," +
                    "ScheduledDepTime, ArrTime, ScheduledArrTime, UniqueCarrier," +
                    "FlightNum, ActualFlightTime, scheduledFlightTime, AirTime," +
                    "ArrDelay, DepDelay, Orig, Dest, Distance) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            String insertAirportsStatement = "INSERT INTO airport(airportCode," +
                    "airportName," +
                    "City," +
                    "State) VALUES (?,?,?,?)";
            PreparedStatement flightStatement = connection.prepareStatement(insertFlightsStatement);
            PreparedStatement airportStatement = connection.prepareStatement(insertAirportsStatement);
            insertFromFile(AIRPORTFILE, airportStatement);
            insertFromFile(DELAYEDFLIGHTSFILE, flightStatement);
        } catch (FileNotFoundException | SQLException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //QUESTION 5
        String query = "SELECT DISTINCT uniquecarrier, COUNT(uniquecarrier) " +
                "FROM delayedflights " +
                "GROUP BY uniquecarrier " +
                "ORDER BY count DESC " +
                "LIMIT 5;";
        ResultSet rs = executeSelect(connection, query);
        try {
            System.out.println("########## 1st Query ##########");
            printResultSet(rs);
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //QUESTION 6
        query = "SELECT airport.city, COUNT(orig) " +
                "FROM airport " +
                "INNER JOIN delayedflights ON airport.airportcode = delayedflights.Orig " +
                "GROUP BY airport.city " +
                "ORDER BY count DESC " +
                "LIMIT 5;";
        rs = executeSelect(connection, query);
        try {
            System.out.println("########## 2nd Query ##########");
            printResultSet(rs);
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //QUESTION 7
        query = "WITH foo AS (SELECT DISTINCT dest, SUM(arrdelay) " +
                "FROM delayedflights " +
                "GROUP BY dest " +
                "ORDER BY sum DESC " +
                "LIMIT 6) " +
                ",bar AS (SELECT * FROM foo " +
                "ORDER BY sum ASC " +
                "LIMIT 5) " +
                "SELECT * FROM bar ORDER BY sum DESC;";
        rs = executeSelect(connection, query);
        try {
            System.out.println("########## 3rd Query ##########");
            printResultSet(rs);
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //QUESTION 8
        query = "WITH orig_flights AS " +
                "(SELECT airport.state, delayedflights.orig FROM airport " +
                "INNER JOIN delayedflights ON airport.airportcode = delayedflights.orig " +
                "), dest_flights AS " +
                "(SELECT airport.state, delayedflights.dest " +
                "FROM airport " +
                "INNER JOIN delayedflights ON airport.airportcode = delayedflights.dest " +
                ") SELECT DISTINCT dest_flights.state, COUNT(dest_flights.dest) " +
                "FROM dest_flights " +
                "INNER JOIN orig_flights ON dest_flights.state = orig_flights.state " +
                "WHERE dest_flights.dest <> orig_flights.orig " +
                "GROUP BY dest_flights.state " +
                "ORDER BY count DESC " +
                "LIMIT 5;";
        rs = executeSelect(connection, query);
        try {
            System.out.println("########## 4th Query ##########");
            printResultSet(rs);
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

