package test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

import org.apache.commons.lang3.tuple.Pair;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;

public class Test {
 /*public static void main(String[] args) {
//	SmbFile file;
//	String user = "ubuntu";
//    String pass ="ubuntu";
//    NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication("",user, pass);
// 
//	try {
//		file = new SmbFile("smb://10.24.24.114/darkflow/",auth);
//		System.out.println(file.getPath());
//		Path directoryPath = Paths.get("smb://10.24.24.114/darkflow/");
//		System.out.println(directoryPath);
//	} catch (Exception e) {
//		// TODO Auto-generated catch block
//		e.printStackTrace();
//	}
	 /*Pair<String,String> pair = Pair.of("image_00000850_0.json", "[{\"label\":\"person\",\"confidence\":0.63,\"topleft\":{\"x\":11,\"y\":97},\"bottomright\":{\"x\":65,\"y\":392}}]");

	 JsonArray jsonArray = (new Gson()).fromJson(pair.getRight(), JsonArray.class);
	 
		for(int i=0;i<jsonArray.size();i++)
		{
			JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
			System.out.println(jsonObject.get("label").getAsString().equals("person"));
			if(jsonObject.get("label").equals("person")) {
				System.out.println("Nailed");
			}

		}
		}
		*/

    public static Connection connect() {
        String url = "jdbc:sqlite:/home/pushkar/workplace/testdb.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void selectAll() {
        String sql = "SELECT * from tblone";

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println(rs.getString("one") + "\t"
                                    + rs.getInt("two"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String []args) {
        selectAll();
    }

}

