package com.mytest.util;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;
import java.util.LinkedHashMap;

import org.apache.commons.io.FileUtils;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;

import com.mytest.pages.setup.datastores.DataStore;

public class TestUtil {

	Xls_Reader xls = new Xls_Reader(Constants.test_data_path);

	// true- test has to be executed
	// false- test has to be skipped
	public static boolean isExecutable(String testName, Xls_Reader xls) {

		for (int rowNum = 2; rowNum <= xls.getRowCount("Test Cases"); rowNum++) {

			if (xls.getCellData("Test Cases", "TCID", rowNum).equals(testName)) {

				if (xls.getCellData("Test Cases", "Runmode", rowNum).equals("Y"))
					return true;
				else
					return false;
			}
			// print the test cases with RUnmode Y
		}

		return false;
	}

	public static void takeScreenShot(String fileName) {
		// Take screenshot
		File srcFile = ((TakesScreenshot) TestBase.driver).getScreenshotAs(OutputType.FILE);
		try {
			FileUtils.copyFile(srcFile,
					new File(System.getProperty("user.dir") + "\\screenshots\\" + fileName + ".jpg"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static Object[][] getData(String testName, Xls_Reader xls) {

		int testStartRowNumber = 0;
		for (int rNum = 1; rNum <= xls.getRowCount("Test Data"); rNum++) {
			if (xls.getCellData("Test Data", 0, rNum).equals(testName)) {

				testStartRowNumber = rNum;
				break;
			}
		}
		//System.out.println(testName + " starts from " + testStartRowNumber);

		int colStartRowNum = testStartRowNumber + 1;
		int totalCols = 0;
		while (!xls.getCellData("Test Data", totalCols, colStartRowNum).equals("")) {
			totalCols++;
		}
		//System.out.println(testName + " total columns are " + totalCols);

		int dataStartRowNum = testStartRowNumber + 2;
		int totalRows = 0;
		while (!xls.getCellData("Test Data", 0, dataStartRowNum + totalRows).equals("")) {
			totalRows++;
		}

		//System.out.println(testName + " total rows in data are " + totalRows);
		//System.out.println("################");

		// put the data in hashtable and put hashtable in array

		Object[][] testdataArray = new Object[totalRows][1];
		int index = 0;
		Hashtable<String, String> table = null;
		for (int rNum = dataStartRowNum; rNum < (dataStartRowNum + totalRows); rNum++) {
			table = new Hashtable<String, String>();
			for (int cNum = 0; cNum < totalCols; cNum++) {
				table.put(xls.getCellData("Test Data", cNum, colStartRowNum), xls.getCellData("Test Data", cNum, rNum));
				//System.out.print(xls.getCellData("Test Data", cNum, rNum) + " -- ");
			}
			testdataArray[index][0] = table;
			index++;
			//System.out.println();
		}

		//System.out.println("done");

		return testdataArray;
	}
	
	
	public static Object[][] getData(String testName, String sheetName, Xls_Reader xls) {

		int testStartRowNumber = 0;
		for (int rNum = 1; rNum <= xls.getRowCount(sheetName); rNum++) {
			if (xls.getCellData(sheetName, 0, rNum).equals(testName)) {

				testStartRowNumber = rNum;
				break;
			}
		}
		//System.out.println(testName + " starts from " + testStartRowNumber);

		int colStartRowNum = testStartRowNumber + 1;
		int totalCols = 0;
		while (!xls.getCellData(sheetName, totalCols, colStartRowNum).equals("")) {
			totalCols++;
		}
		//System.out.println(testName + " total columns are " + totalCols);

		int dataStartRowNum = testStartRowNumber + 2;
		int totalRows = 0;
		while (!xls.getCellData(sheetName, 0, dataStartRowNum + totalRows).equals("")) {
			totalRows++;
		}

		//System.out.println(testName + " total rows in data are " + totalRows);
		//System.out.println("################");

		// put the data in hashtable and put hashtable in array

		Object[][] testdataArray = new Object[totalRows][1];
		int index = 0;
		Hashtable<String, String> table = null;
		for (int rNum = dataStartRowNum; rNum < (dataStartRowNum + totalRows); rNum++) {
			table = new Hashtable<String, String>();
			for (int cNum = 0; cNum < totalCols; cNum++) {
				table.put(xls.getCellData(sheetName, cNum, colStartRowNum), xls.getCellData(sheetName, cNum, rNum));
				//System.out.print(xls.getCellData("Test Data", cNum, rNum) + " -- ");
			}
			testdataArray[index][0] = table;
			index++;
			//System.out.println();
		}

		//System.out.println("done");

		return testdataArray;
	}

	///
	/**
	 * Method to set Data of the DataStore Object into xls File
	 * 
	 * @param dstore
	 *            Data Store Object with all the Profiling Data set in it
	 * @param xls
	 *            - File in which the data is written
	 */
	public static void setData(DataStore dstore, Xls_Reader profilingoutputxls) {

		profilingoutputxls.setCellData("BasicDataProfilingData", "DataStoreName", 2, dstore.getDatastoreName());
		profilingoutputxls.setCellData("BasicDataProfilingData", "DatastoreDescription", 2,
				dstore.getDatastoreDescription());
		profilingoutputxls.setCellData("BasicDataProfilingData", "WarehouseDirectory", 2,
				dstore.getWarehouseDirectory());
		profilingoutputxls.setCellData("BasicDataProfilingData", "FileSystem", 2, dstore.getFileSystem());
		profilingoutputxls.setCellData("BasicDataProfilingData", "MetastoreHost", 2, dstore.getMetastoreHost());
		profilingoutputxls.setCellData("BasicDataProfilingData", "MetastorePort", 2, dstore.getMetastorePort());
		profilingoutputxls.setCellData("BasicDataProfilingData", "JobHistoryServerUrl", 2,
				dstore.getJobHistoryServerUrl());
		profilingoutputxls.setCellData("BasicDataProfilingData", "DatabasesCount", 2, dstore.getDatabasesCount());

		/// Database level
		profilingoutputxls.setCellData("BasicDataProfilingData", "DatabaseName", 2, dstore.getDatabaseName());
		profilingoutputxls.setCellData("BasicDataProfilingData", "DatabaseOwnerName", 2, dstore.getDatabaseOwnerName());
		profilingoutputxls.setCellData("BasicDataProfilingData", "DatabaseLocation", 2, dstore.getDatabaseLocation());
		profilingoutputxls.setCellData("BasicDataProfilingData", "TableCount", 2, dstore.getTableCount());

	}
	
	
	// get data from Technical View sheet
	public static LinkedHashMap<String, String> getBrowseData(String viewName, String testName, Xls_Reader xls) {

		int testStartRowNumber = 0;
		for (int rNum = 1; rNum <= xls.getRowCount(viewName); rNum++) {
			if (xls.getCellData(viewName, 0, rNum).equals(testName)) {
				testStartRowNumber = rNum;
				break;
			}
		}
	
		int colStartRowNum = testStartRowNumber + 1;
		int totalCols = 0;
		while (!xls.getCellData(viewName, totalCols, colStartRowNum).equals("")) {
			totalCols++;
		}
		
		LinkedHashMap<String, String> table = new LinkedHashMap<String, String>();
		for (int cNum = 0; cNum < totalCols; cNum++) {
			table.put(xls.getCellData(viewName, cNum, colStartRowNum), xls.getCellData(viewName, cNum, colStartRowNum+1));
			//System.out.print(xls.getCellData("Test Data", cNum, rNum) + " -- ");
		}
			
		return table;
	}
	
	// get data from Technical View sheet
		public static LinkedHashMap<String, String> getDataWithRowNum(String testName, Xls_Reader xls, int rowNum) {

			int testStartRowNumber = 0;
			for (int rNum = 1; rNum <= xls.getRowCount("TechnicalView"); rNum++) {
				if (xls.getCellData("TechnicalView", 0, rNum).equals(testName)) {
					testStartRowNumber = rNum;
					break;
				}
			}
		
			int colStartRowNum = testStartRowNumber + 1;
			int totalCols = 0;
			while (!xls.getCellData("TechnicalView", totalCols, colStartRowNum).equals("")) {
				totalCols++;
			}
			
			LinkedHashMap<String, String> table = new LinkedHashMap<String, String>();
			for (int cNum = 0; cNum < totalCols; cNum++) {
				table.put(xls.getCellData("TechnicalView", cNum, colStartRowNum), xls.getCellData("TechnicalView", cNum, colStartRowNum+rowNum));
				//System.out.print(xls.getCellData("Test Data", cNum, rNum) + " -- ");
			}
				
			return table;
		}
		
		

}
