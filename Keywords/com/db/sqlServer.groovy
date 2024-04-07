package com.db

import static com.kms.katalon.core.checkpoint.CheckpointFactory.findCheckpoint
import static com.kms.katalon.core.testcase.TestCaseFactory.findTestCase
import static com.kms.katalon.core.testdata.TestDataFactory.findTestData
import static com.kms.katalon.core.testobject.ObjectRepository.findTestObject
import static com.kms.katalon.core.testobject.ObjectRepository.findWindowsObject

import com.kms.katalon.core.annotation.Keyword
import com.kms.katalon.core.checkpoint.Checkpoint
import com.kms.katalon.core.cucumber.keyword.CucumberBuiltinKeywords as CucumberKW
import com.kms.katalon.core.mobile.keyword.MobileBuiltInKeywords as Mobile
import com.kms.katalon.core.model.FailureHandling
import com.kms.katalon.core.testcase.TestCase
import com.kms.katalon.core.testdata.TestData
import com.kms.katalon.core.testobject.TestObject
import com.kms.katalon.core.webservice.keyword.WSBuiltInKeywords as WS
import com.kms.katalon.core.webui.keyword.WebUiBuiltInKeywords as WebUI
import com.kms.katalon.core.windows.keyword.WindowsBuiltinKeywords as Windows

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import internal.GlobalVariable

public class sqlServer {

	private static Connection connection = null;

	//url: DESKTOP-74788IK\\SQLEXPRESS:1433
	//dbname: test_connent_db_with_katalon
	//username: sa
	//password: 123456

	@Keyword
	///////////////// Connect DB /////////////////////////
	def connectDB(String url, String dbname, String username, String password) {
		String conn = "jdbc:sqlserver://" + url + ";" + "databaseName=" + dbname +";" +
				"integratedSecurity=true;" +
				"encrypt=true;trustServerCertificate=true";
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		connection = DriverManager.getConnection(conn, username, password);
		return connection
	}

	@Keyword
	///////////////// Execute Query ////////////////
	def executeQuery(String queryString) {
		//		String sql = "select * from CAR";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(queryString);
		ResultSetMetaData metadata = rs.getMetaData();
		int columnCount = metadata.getColumnCount();
		List<List<String>> rowList = new LinkedList<List<String>>();
		while (rs.next()) {
			List<String> row = new LinkedList<>();
			for (int i = 1; i <= columnCount; i++) {
				Object value = rs.getObject(i);
				row.add(value.toString());
			}
			rowList.add(row);
		}

		for (List<String> row : rowList) {
			for (String data : row) {
				System.out.print(data + "\t"); // Use tab (\t) for spacing between columns
			}
			System.out.println(); // Add a new line after each row
		}
		return rowList
	}

	@Keyword
	///////////////// closing the connection ////////////////
	def closeDbConnect() {
		if(connection !=null && !connection.isClosed()) {
			connection.close()
		}
		connection = null
	}
}
