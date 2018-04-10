package com.mytest.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.ie.InternetExplorerDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.PageFactory;

import com.mytest.util.Constants;
import com.mytest.pages.dashboard.LandingPage;
import com.mytest.pages.login.LoginPage;
import com.mytest.pages.TopMenu;
import com.mytest.util.Xls_Reader;


//singleton class
//TODO make the class abstract
public abstract class  TestBase {

	//TODO change access modifiers to least (private or protected)
	protected static Logger application_logs = null;
	protected static WebDriver driver = null;
	protected static Properties config = null;
	protected static Properties csvMessages = null;
	protected static TopMenu topmenu = null;
	protected static boolean isLoggedIn = false;
	protected static boolean isLoggedInUpgrade = false;
	protected static Connection con = null;
	protected Statement stmt = null;
	
	static Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
	protected static long ts1 = timestamp1.getTime();
	protected static long ts2 = ts1+1;
	protected static long ts3 = ts1+2;
	protected static long ts4 = ts1+3;
	protected static long ts5 = ts1+4;
	//protected String baseURL = getConfig().getProperty("testSiteName");
	
	
	
	//TODO make this path configurable
	protected Xls_Reader xls = new Xls_Reader(Constants.test_data_path);
	protected Xls_Reader xls1 = new Xls_Reader(Constants.test_data_path_1);
	protected Xls_Reader profilingoutputxls = new Xls_Reader(Constants.outPuttest_data_path);
	protected static Xls_Reader apixls = new Xls_Reader(Constants.api_test_path);
	
	//TODO make this path configurable
	//TODO change access modifiers to least (private or protected)
	protected void initConfig() throws IOException{
	//TODO implement logger 
	if(config==null){
		application_logs = Logger.getLogger("devpinoyLogger");
	
		//TODO load using PropertiesUtil
		//TODO change case for variable name
		config = new Properties();
		try{
			FileInputStream fs = new FileInputStream(Constants.config_file_path);
			config.load(fs);
		} catch (Throwable e){
		ErrorUtil.addVerificationFailure(e);
			}
		}	
	if (csvMessages == null) {
		application_logs = Logger.getLogger("devpinoyLogger");

		// TODO load using PropertiesUtil
		// TODO change case for variable name
		csvMessages = new Properties();
		try {
			FileInputStream fs = new FileInputStream(Constants.csv_messages_file_path);
			csvMessages.load(fs);
		} catch (Throwable e) {
			ErrorUtil.addVerificationFailure(e);
		}
	}

}

	//TODO multi threaded safe
	
	protected void initDriver(){
		if (driver == null){
			if("firefox".equalsIgnoreCase(config.getProperty("browser"))){
				driver = new FirefoxDriver();
			}
			 else if ("chrome".equalsIgnoreCase(config.getProperty("browser"))) {

					System.setProperty("webdriver.chrome.driver", Constants.chromeDriver_path);
					String downloadFilepath = Constants.CSV_DOWNLOAD_PATH;
					HashMap<String, Object> chromePrefs = new HashMap<String, Object>();
					chromePrefs.put("profile.default_content_settings.popups", 0);
					chromePrefs.put("download.default_directory", downloadFilepath);
					ChromeOptions options = new ChromeOptions();
					options.setExperimentalOption("prefs", chromePrefs);
					DesiredCapabilities cap = DesiredCapabilities.chrome();
					cap.setCapability(CapabilityType.ACCEPT_SSL_CERTS, true);
					cap.setCapability(ChromeOptions.CAPABILITY, options);
					driver = new ChromeDriver(cap);
					/* driver = new ChromeDriver(); */
			}
			else if("ie".equalsIgnoreCase(config.getProperty("browser"))){
				driver = new InternetExplorerDriver();
			}
		}
	}
	
    //TODO create a DBUtil which will execute SQLs and return result 
    protected void dbInit() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException{
		
		//static String JDBCDriver = "org.apache.hive.jdbc.HiveDriver";
		//TODO define in config.properties
		String url = config.getProperty("databaseUrl");
		String username = config.getProperty("dbDefaultUsername");
		String password = config.getProperty("dbDefaultPassword");
		
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
		/*	con = DriverManager.getConnection(url, username, password);
								
			stmt = con.createStatement();
		*/
	}
	
	
    public static Boolean checkNotification(String Notification_msg_exp) throws InterruptedException, IOException {
		// validate notification
		boolean isNotified = false;

		driver.findElement(By.xpath("/html/body/div[1]/div[1]/nav/div/ul[2]/li[1]/a[1]/noti-popup/span")).click();
		Thread.sleep(2000L);
		driver.findElement(By.xpath("//*[@id='hoursChart3']/div/div")).click();
		String Notification_msg_act = driver.findElement(By.xpath("//*[@id='top-slider-content']/div[1]/div/div[5]/span[4]")).getText();
		System.out.println("Actual notification message is ->  "+Notification_msg_act);
		if (Notification_msg_act.equalsIgnoreCase(Notification_msg_exp)) {
			isNotified = true;
		}
		
		Thread.sleep(3000);
		driver.findElement(By.xpath("//*[@id='top-slider-content']/div[1]/div/div[4]/i[1]")).click();
		
		return isNotified;
	}
	
  
	//TODO remove static, public if not required
	public static void quitDriver(){
		driver.quit();
		driver=null;
	}
	
	public static TopMenu getTopMenu(){
		if (topmenu == null){
		topmenu = PageFactory.initElements(driver, TopMenu.class);
		}
		return topmenu;
		
	}
	
	
	protected void logOut(){
		driver.quit();
		driver=null;
		isLoggedIn=false;
		isLoggedInUpgrade=false;
	}
	
	
	public static Properties getConfig(){
		return config;
	}
	
	protected static Boolean isLoggedIn(){
		return isLoggedIn;
	}
	
	public static void setLoggedIn(Boolean loggedIn){
		isLoggedIn = loggedIn;
	}
	
	public static void setLoggedInUpgrade(Boolean loggedInUpgrade){
		isLoggedInUpgrade = loggedInUpgrade;
	}
	
	protected static String attachTimeStamp(String name){
				
		if (name.substring(name.length()-1).equalsIgnoreCase("1")){
			System.out.println("TS1 = " + ts1);
			return (name.substring(0, name.length()-2)+ts1);
		}
			
		if (name.substring(name.length()-1).equalsIgnoreCase("2")){
			System.out.println("TS2 = " + ts2);
			return (name.substring(0, name.length()-2)+ts2);
		}
			
		if (name.substring(name.length()-1).equalsIgnoreCase("3")){
			System.out.println("TS3 = " + ts3);
			return (name.substring(0, name.length()-2)+ts3);
		}
			
		if (name.substring(name.length()-1).equalsIgnoreCase("4"))
			return (name.substring(0, name.length()-2)+ts4);
		if (name.substring(name.length()-1).equalsIgnoreCase("5"))
			return (name.substring(0, name.length()-2)+ts1);
		return "Failed";
	}
	
}
