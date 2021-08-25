import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBC {
    public static void main(String[] args) throws Exception {

        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306", "root", "root");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select * from employees.departments");
        while (rs.next())
            System.out.println(rs.getString(1) + "  " + rs.getString(2));
        con.close();
    }
}