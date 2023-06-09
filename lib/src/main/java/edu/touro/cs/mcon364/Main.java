package edu.touro.cs.mcon364;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    static HashSet<String> visited = new HashSet<>();
    static HashSet<String> emails = new HashSet<>();
    static ArrayList<Object[]> sqlInserts = new ArrayList<>();

//    static Logger submitLogger = LoggerFactory.getLogger("submit");
//    static Logger debugLogger = LoggerFactory.getLogger("debug");

    static final String DATABASE_URL = "";
    static final String USERNAME ="";
    static final String PASSWORD = "";

    static final String [] banned = {"touroscholar","ebsco","flickr","youtu.be", "pdf", "jpeg", "png", "jpg", "css", "js","vimeo","twitter", "facebook", "linkedin", "w3", "youtube", "google", "instagram", "github", "#", "mailto", ".gov", "tel:"};




    static  ExecutorService executorService = Executors.newFixedThreadPool(100);

    public static void main(String[] args) {
        System.setProperty("javax.net.ssl.trustStore", "/Users/aharon/Library/Java/JavaVirtualMachines/openjdk-19.0.1/Contents/Home/lib/security/cacerts");

        String trustStorePath = System.getProperty("javax.net.ssl.trustStore");
        System.out.println("Current TrustStore path: " + trustStorePath);


        executorService.execute(()->clickAndHarvest(null));
//        try {
//            BufferedReader bufferedReader = new BufferedReader(new FileReader("banned.txt"));
//            String s;
//            while((s = bufferedReader.readLine()) != null){
//                banned.add(s);
//                System.out.println(s + " is BANNED!!");
//            }
//        } catch (IOException e) {
//            e.getStackTrace();
//        }
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            injectIntoDatabase();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void clickAndHarvest(String w){
        Connection driver;

        if (emails.size() >= 10_000){
            System.out.println("FINISHED!!");

            executorService.shutdown();
            return;
        }
        if( w== null){
            w = "https://www.touro.edu/";

        }
//        if(banned.contains(w)){
//            System.out.println("~~skipping " + w);
//            return;
//        }

        synchronized (Main.class) {
            if (visited.contains(w)) {
                return;

            } else {
                driver = Jsoup.connect(w);
//                        .proxy("134.209.233.38",8080);
//                debugLogger.info(("Visiting: " + w  + (visited.size() + 1) + " links visited + " + emails.size() + " emails so far"));
                System.out.println(("Visiting: " + w  + (visited.size() + 1) + " links visited + " + emails.size() + " emails so far"));
            }
        }






        Pattern p = Pattern.compile("[_a-zA-Z0-9.]+@[a-zA-Z]+[.][a-zA-Z]{2,}");
        Document document = null;

        try {
            document = driver.get();
        } catch (IOException e) {
            System.out.println(e.getLocalizedMessage());
            return;
//           throw new RuntimeException(e);

        }
        String html = document.html();
        Matcher m = p.matcher(html);

        synchronized (Main.class) {
//            boolean found = false;
            while (m.find()) {

                String email = m.group();
//                    if (emails.add(email)){
////                        found = true;
//                    }
                if (emails.add(email.toLowerCase())) {

//                        debugLogger.info(email + " at " + w + " " + emails.size() + " emails so far");
//                        submitLogger.info(email + " at " + w + " " + emails.size() + " emails so far");
                    System.out.println(email + " at " + w + " " + emails.size() + " emails so far");
                    Object[] insertParams = {email, w, new Timestamp(System.currentTimeMillis())};
                    sqlInserts.add(insertParams);
                }
            }

//                if(!found && !w.equals("https://www.touro.edu/")){
//                    try {
//                        FileWriter fw = new FileWriter("banned.txt", true);
//                        System.out.println("Banning " + w+ "!!!!!");
//                        fw.write(w+"\n");
//                        fw.close();
//                    }
//
//                    catch (Exception e) {
//                        e.getStackTrace();
//                    }
//                }

        }

        Elements links = document.select("a[href]");

        for (Element link : links){
            String linkString = link.absUrl("href");

            if(linkString.startsWith("http") && Arrays.stream(banned).noneMatch(linkString.toLowerCase()::contains)) {
                executorService.execute(() -> clickAndHarvest(linkString));
            }
        }
        visited.add(w);


    }

    static void injectIntoDatabase() {
        String endpoint = "mcon364.ckxf3a0k0vuw.us-east-1.rds.amazonaws.com";
        System.out.println(endpoint);
        String connectionUrl = // specifies how to connect to the database
                "jdbc:sqlserver://" + endpoint + ";"
                        + "database=Seidman_Aharon;"
                        + "user=admin;"
                        + "password=mcon364_417;"
                        + "encrypt=false;"
                        + "trustServerCertificate=false;"
                        + "loginTimeout=30;";

        try (java.sql.Connection connection = DriverManager.getConnection(connectionUrl); // AutoCloseable
             Statement statement = connection.createStatement();
             ResultSet rs = connection.getMetaData().getTables(null, null, "Emails", null);) {
            if(!rs.next()) {
                String sqlStatement = "CREATE TABLE Emails (EmailID INT identity(1,1) PRIMARY KEY, EmailAddress NVARCHAR(255) NOT NULL, Source NVARCHAR(255) NOT NULL, TimeStamp DATETIME NOT NULL);";
                statement.execute(sqlStatement);
//                debugLogger.info("made table emails");
                System.out.println("made table emails");
            } else {
//                debugLogger.info("table email exists");
                System.out.println("table email exists");
            }
        } catch (SQLException e) {
//            debugLogger.error("Error: ", e);
            System.out.println(e.getLocalizedMessage());
        }


        try (java.sql.Connection connection = DriverManager.getConnection(connectionUrl)) {
            {
                String insertStatement = "INSERT INTO Emails (EmailAddress, Source, TimeStamp) VALUES (?,?,?)";
                PreparedStatement statement = connection.prepareStatement(insertStatement);

                for(Object[] insertParams : sqlInserts){
                    statement.setString(1, String.valueOf(insertParams[0]) );
                    statement.setString(2, String.valueOf(insertParams[1]) );
                    statement.setTimestamp(3, (Timestamp)insertParams[2]);
                    statement.addBatch();
                }

                statement.executeBatch();
//                debugLogger.info("Inserted all sql and DONE!!");
                System.out.println("Inserted all sql and DONE!!");


            }} catch (SQLException e) {
//            debugLogger.error("ERROR: ", e);
            System.out.println(e.getLocalizedMessage());
        }
    }}
