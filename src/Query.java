import java.lang.System;
import java.util.Properties;
import java.sql.*;

public class Query {

	Connection conn = null;
	Statement stmt  = null;
	Statement stmt2  = null;
	Statement stmt3  = null;
	PreparedStatement instructorPStmt = null;
	CallableStatement instructorCStmt = null;
	static String instructorQuery = "SELECT * from instructor";
	static String instructorCnt = "CALL Count_Courses(?,?)";

	/**
	 * University Main
	 */
	public static void main(String[] args) 
	{
		String userName = args[0];	// Input User Name for creating connection to University Schema
		String passWord = args[1];	// Input Password for creating connection to University Schema

		try
		{
			// Invoke the University constructor, passing in User Name and Password.
			Query university = new Query(userName, passWord);

			// Display the contents of the Instructor table,
			// first using a mere Statement object, with the query built in this method,
			// and then second, using a Prepared statement doing the same thing. 
			System.out.println("caseOneFullStatement ...");
			university.caseOneFullStatement();
			System.out.println("caseOneJavaStatement ...");
			university.caseOneJavaStatement();
			System.out.println("caseTwoFullStatement ...");
			university.caseTwoFullStatement();
			System.out.println("caseTwoJavaStatement ...");
			university.caseTwoJavaStatement();
			university.cleanup();
		}
		catch (SQLException exSQL)
		{
			System.out.println("main SQLException: " + exSQL.getMessage());
			System.out.println("main SQLState: " + exSQL.getSQLState());
			System.out.println("main VendorError: " + exSQL.getErrorCode());
			exSQL.printStackTrace();
		}
	}

	/**
	 * Constructor for Class University
	 * @param String userName for connecting to University schema
	 * @param String passWord for connecting to University schema
	 */
	public Query(String userName, String passWord) throws SQLException
	{		
		// Go create a connection to my "university" database.
		// "conn" is a data member of JDBC type Connection.
		// See University::getConnection method below.
		conn = getConnection(userName, passWord);
		if (conn == null)
		{
			System.out.println("getConnection failed.");
			return;
		}

		// Create Statement object for use in creating the non-prepared queries.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/Connection.html
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/Statement.html
		stmt = conn.createStatement();
		stmt2 = conn.createStatement();
		stmt3 = conn.createStatement();

		// Create the Prepared statements.
		// A SQL statement is precompiled and stored in a PreparedStatement object.
		// This object can then be used to efficiently execute this statement multiple times,
		// rather than having it recompiled with each execution.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/PreparedStatement.html
		instructorPStmt = conn.prepareStatement(instructorQuery, ResultSet.FETCH_FORWARD);

		// Create a Callable statement to call a stored procedure returning a course count.
		// http://pages.cs.wisc.edu/~hasti/cs368/JavaTutorial/jdk1.2/api/java/sql/CallableStatement.html
		instructorCStmt = conn.prepareCall(instructorCnt);

	}	// End of University Constructor.

	/**
	 * Method: cleanup()
	 * Function: To close the various JDBC objects.
	 */
	public void cleanup()
	{
		try {
			stmt.close();
			conn.close();
			instructorPStmt.close();
		}
		catch  (SQLException exSQL) {;}
	}

	public Connection getConnection(String userName, String passWord)
	{
		Connection conn = null;

		// Location of the MySQL-based "university" database.
		String university_url = "jdbc:mysql://localhost:3306/university";

		// Load the JDBC driver manager.
		// http://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
		// http://dev.mysql.com/doc/refman/5.5/en/connector-j-usagenotes-connect-drivermanager.html
		// The name of the class that implements java.sql.Driver in MySQL Connector/J is "com.mysql.jdbc.Driver".
		// The DriverManager needs to be told which JDBC driver with which it should try to make a Connection.
		// One way to do this is to use Class.forName() on the class that implements the java.sql.Driver interface.
		// With MySQL Connector/J, the name of this class is com.mysql.jdbc.Driver.
		try { Class.forName("com.mysql.jdbc.Driver").newInstance(); }
		catch (Exception ex) { 
			System.out.println("Class.forName Exception: " + ex.getMessage());
			ex.printStackTrace();
			return null;
		}

		// Construct a Properties object for passing the User Name and Password into the DB connection.
		// Create the DB connection.
		// http://www.tutorialspoint.com/jdbc/jdbc-db-connections.htm
		try {
			Properties connectionProps = new Properties();
			connectionProps.put("user", userName);
			connectionProps.put("password", passWord);
			conn = DriverManager.getConnection (university_url, connectionProps);
			if (conn == null)
			{
				System.out.println("getConnection:getConnection failed.");
				return null;
			}
		}
		catch (SQLException exSQL)
		{
			System.out.println("getConnection SQLException: " + exSQL.getMessage());
			System.out.println("getConnection SQLState: " + exSQL.getSQLState());
			System.out.println("getConnection VendorError: " + exSQL.getErrorCode());
			exSQL.printStackTrace();
		}

		return conn;
	}	// End of getConnection()


	void caseOneFullStatement() throws SQLException {
		instructorQuery = ""
				+ "SELECT dept_name, COUNT(*) AS CNT "
				+ "FROM student GROUP BY dept_name "
				+ "HAVING CNT = (SELECT MAX(ACNT) AS MAXCNT "
				+ "FROM (SELECT dept_name, COUNT(*) AS ACNT "
				+ "FROM student GROUP BY dept_name) AS ATable)";

		// Send the statement to be executed
		ResultSet rs = stmt.executeQuery(instructorQuery);

		while(rs.next())
		{
			// This will print the result, even though it's in a loop there is only one result
			System.out.format("%-20s%-20s%n%-20s%-20d%n%n", "Department name:", "Total Count:", 
					rs.getString("dept_name"), rs.getInt("cnt"));
		}

		rs.close();
	}	// End of caseOneFullStatement()

	void caseOneJavaStatement() throws SQLException {
		
		instructorQuery = ""
				+ "SELECT dept_name, COUNT(*) FROM student GROUP BY dept_name";

		// Send the statement to be executed
		ResultSet rs = stmt.executeQuery(instructorQuery);

		String output = "";
		int count = 0;
		int newInt = 0;
		while(rs.next()) {
			newInt = rs.getInt("count(*)");
			if (newInt > count) {
				count = newInt;
				output = String.format("%-20s%-20d%n" , rs.getString("dept_name"), count);
			} else if (newInt == count){
				output = output + (String.format("%-20s%-20d%n" , rs.getString("dept_name"), newInt));
			}
		}
		// This will print the result, even though it's in a loop there is only one result
		output = (String.format("%-20s%-20s%n", "Department name:", "Total Count:") + output);
		System.out.println(output);

		rs.close();
	}	// End of caseOneJavaStatement()

	void caseTwoFullStatement() throws SQLException
	{
		instructorQuery = ""
				+ "SELECT instructor.name, student.name, instructor.dept_name "
				+ "FROM advisor JOIN instructor JOIN student "
				+ "WHERE advisor.i_ID = instructor.ID AND advisor.s_ID = student.ID";

		// Send the statement to be executed
		ResultSet rs = stmt.executeQuery(instructorQuery);

		String output = String.format("%-20s%-20s%-20s%n", "Instructor Name:", "Student name:","Department name:");
		
		while(rs.next())
		{
			String instructorName = rs.getString("instructor.name");
			String studentName = rs.getString("student.name");
			String departmentName = rs.getString("dept_name");

			// This will print the result, even though it's in a loop there is only one result
			output = output + String.format("%-20s%-20s%-20s%n"
					, instructorName, studentName, departmentName);
		}

		System.out.println(output);
		
		rs.close();
	} // End of caseTwoFullStatement()
	
	void caseTwoJavaStatement() throws SQLException
	{
		instructorQuery = ""
				+ "SELECT s_ID, i_ID FROM advisor;";
		

		// Send the statement to be executed
		ResultSet rs = stmt.executeQuery(instructorQuery);
		ResultSet irs = null;
		ResultSet srs = null;
		
		String output = String.format("%-20s%-20s%-20s%n", "Instructor Name:", "Student name:","Department name:");
		
		rs.beforeFirst();
		
		while(rs.next())
		{
			String instructorID = rs.getString("i_ID");
			String studentID = rs.getString("s_ID");
			
			instructorQuery = "SELECT name, dept_name FROM instructor WHERE ID = " + instructorID + ";";
			irs = stmt2.executeQuery(instructorQuery);
			irs.first();
			
			instructorQuery = "SELECT name FROM student WHERE ID = " + studentID + ";";
			srs = stmt3.executeQuery(instructorQuery);
			srs.first();
			
			output = output + String.format("%-20s%-20s%-20s%n"
					, irs.getString("name"), srs.getString("name"), irs.getString("dept_name"));
		}

		System.out.println(output);
		
		rs.close();
		irs.close();
		srs.close();
	} // End of caseTwoJavaStatement()

}	// End of Query Class
