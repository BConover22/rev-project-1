package proj1

import P1Menu.Menu
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.ListMap
import scala.io.StdIn.readLine

object P1App {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder().appName("P1App").config("spark.master", "local").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //START : TABLE CREATION  --------------------------------------------------
    // BevByBranch table contains data from Bev_Branch files
    spark.sql("CREATE TABLE IF NOT EXISTS BevByBranch(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BevByBranch")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE BevByBranch")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE BevByBranch")
    // BevByCount table contains data from Bev_Conscount files
    spark.sql("CREATE TABLE IF NOT EXISTS BevByCount(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountA.txt' OVERWRITE INTO TABLE BevByCount")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountB.txt' INTO TABLE BevByCount")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountC.txt' INTO TABLE BevByCount")
    // BevBranchA table
    spark.sql("CREATE TABLE IF NOT EXISTS BevBranchA(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' OVERWRITE INTO TABLE BevBranchA")
    // BevBranchB table
    spark.sql("CREATE TABLE IF NOT EXISTS BevBranchB(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' OVERWRITE INTO TABLE BevBranchB")
    // BevBranchC table
    spark.sql("CREATE TABLE IF NOT EXISTS BevBranchC(beverages STRING, branches STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' OVERWRITE INTO TABLE BevBranchC")
    // BevCountA table
    spark.sql("CREATE TABLE IF NOT EXISTS BevCountA(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountA.txt' OVERWRITE INTO TABLE BevCountA")
    // BevCountB table
    spark.sql("CREATE TABLE IF NOT EXISTS BevCountB(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountB.txt' OVERWRITE INTO TABLE BevCountB")
    // BevCountC table
    spark.sql("CREATE TABLE IF NOT EXISTS BevCountC(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountC.txt' OVERWRITE INTO TABLE BevCountC")
    // BevCountAC table
    spark.sql("CREATE TABLE IF NOT EXISTS BevCountAC(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountA.txt' OVERWRITE INTO TABLE BevCountAC")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountC.txt' INTO TABLE BevCountAC")
    // RemovingRowTable table
    spark.sql("CREATE TABLE IF NOT EXISTS RemovingRowTable(beverages STRING, counts INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConsCountB.txt' OVERWRITE INTO TABLE RemovingRowTable")

    //END: TABLE CREATION  --------------------------------------------------
    //ACTUAL PROGRAM/APP/THING...
    val tempSelections = ListMap(
      1 -> "Run Scenario 1",
      2 -> "Run Scenario 2",
      3 -> "Run Scenario 3",
      4 -> "Run Scenario 4",
      5 -> "Run Scenario 5",
      6 -> "Run Scenario 6",
      0 -> "finish")
    val menu = new Menu(tempSelections)
    var userSelection = ""

    while (userSelection != "finish") {
      menu.printMenu()
      userSelection = readLine("What would you like to do?  ")
        val m_Options = menu.userOptions(userSelection.toInt)
        m_Options match {
          case "Run Scenario 1" => ProblemOne()
          case "Run Scenario 2" => ProblemTwo()
          case "Run Scenario 3" => ProblemThree()
          case "Run Scenario 4" => ProblemFour()
          case "Run Scenario 5" => ProblemFive()
          case "Run Scenario 6" => ProblemSix()
          case "finish" => userSelection = "finish"
        }


      // * * FUNCTIONS FOR ALL PROBLEMS HERE * *
      // Problem Scenario 1
      def ProblemOne(): Unit = {
        println("What is the total number of consumers for Branch1?")
        // ONLY need BranchA (Branch1 only exists on BranchA)
        spark.sql("SELECT SUM(BevCountA.counts) AS Total_Consumers_Branch1 FROM BevCountA " +
          "JOIN BevByBranch AS b ON BevCountA.beverages = b.beverages WHERE b.branches='Branch1'").show(20)
        println("")
        println("What is the number of consumers for the Branch2?")
        spark.sql("SELECT SUM(BevCountAC.counts) AS Total_Consumers_Branch2 FROM BevCountAC " +
          "JOIN BevByBranch AS b ON BevCountAC.beverages = b.beverages WHERE b.branches ='Branch2'").show(20)
        println("")
      }

      // Problem Scenario 2
      def ProblemTwo(): Unit = {
        println("What is the most consumed beverages on Branch1?")
        spark.sql("SELECT BevByBranch.beverages, sum(BevByCount.counts) FROM BevByBranch " +
          "JOIN BevByCount ON BevByBranch.beverages = BevByCount.beverages WHERE branches ='Branch1' " +
          "GROUP BY BevByBranch.beverages ORDER BY sum(BevByCount.counts) DESC LIMIT 1").show()
        println("")
        println("What is the least consumed beverages on Branch2? ")
        spark.sql("SELECT BevByBranch.beverages, sum(BevByCount.counts) FROM BevByBranch " +
          "JOIN BevByCount ON BevByBranch.beverages = BevByCount.beverages WHERE branches ='Branch2' " +
          "GROUP BY BevByBranch.beverages ORDER BY sum(BevByCount.counts) ASC LIMIT 1").show()
        println("")
      }

      def ProblemThree(): Unit = {
        // Problem Scenario 3
        println("What are the beverages available on Branch10, Branch8, and Branch1?")
        spark.sql("SELECT DISTINCT beverages, branches FROM BevByBranch " +
          "WHERE branches ='Branch8' OR branches ='Branch1' or branches ='Branch10' ORDER BY branches ASC").show(200)
        println("")
        println("Most common beverages available on Branch4 or Branch7")
        spark.sql("SELECT first(BevByBranch.beverages), counts FROM BevByBranch " +
          "JOIN BevByCount ON BevByBranch.beverages = BevByCount.beverages " +
          "WHERE BevByBranch.branches='Branch4' OR BevByBranch.branches='Branch7' " +
          "GROUP BY counts ORDER BY counts DESC ").show()
      }

      // Problem Scenario 4
      def ProblemFour(): Unit = {
        println("Create a partition,View for the scenario3")
        spark.sql("CREATE TABLE IF NOT EXISTS Part_Branch(beverages STRING) PARTITIONED BY (branches STRING)")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("INSERT OVERWRITE TABLE Part_Branch PARTITION(branches) SELECT beverages, branches from BevByBranch")
        spark.sql("SELECT * FROM Part_Branch").show(200)
        println("This table is now partitioned.")

      }


      // Problem Scenario 5
      def ProblemFive(): Unit = {
        // Problem Scenario 5: Alter the table properties to add "note","comment"
        println("Alter the table properties to add 'note', 'comment'")
        spark.sql("ALTER TABLE BevBranchA SET TBLPROPERTIES ('notes' = 'Take NOTE of this')")
        spark.sql("SHOW TBLPROPERTIES BevBranchA").show()

        // Remove a row from the any Senario.
        println("Table before Delete:")
        spark.sql("SELECT * FROM RemovingRowTable").show()
        //new (copy) table
        spark.sql("CREATE TABLE IF NOT EXISTS RemovingRowTableTemp LIKE RemovingRowTable")
        //load all EXCEPT deleted item
        spark.sql("INSERT INTO RemovingRowTableTemp SELECT * FROM RemovingRowTable " +
          "WHERE RemovingRowTable.beverages NOT IN " +
          "(SELECT RemovingRowTable.beverages FROM RemovingRowTable WHERE RemovingRowTable.beverages='Special_Lite')")
        //copy table -> og table
        spark.sql("INSERT OVERWRITE TABLE RemovingRowTable SELECT * FROM RemovingRowTableTemp")
        //drop temp table
        spark.sql("DROP TABLE RemovingRowTableTemp")
        //row has been "deleted"
        println("Table after Delete:")
        spark.sql("SELECT * FROM RemovingRowTable").show()


      }


      def ProblemSix(): Unit = {
        // Problem Scenario 6...kinda (no actual queries, use this to build and introduce #6
        // Future Query: Least Performing Drink
        // Step 1: A is 2019 | B is 2020 | C is 2021
        // Step 2: Sum of conscount for each beverage for A2019 / B2020 / C2021
        //          -- Order by Dsc (Limit 5): SHOW
        // Step 3: GRAPH and ANALYZE
        //--------------------------------------------------
        // YearA : BevCountA
        println("Future Query: What beverage will most likely need to be dropped by Branch 2 in 2023?")
        println("")
        println("Lowest Performing Beverages Chain-wide in 2019")
        spark.sql("SELECT beverages FROM BevCountA GROUP BY beverages ORDER BY sum(counts) ASC LIMIT 10").show()
        // YearB : BevCountB
        println("Lowest Performing Beverages Chain-wide in 2020")
        spark.sql("SELECT beverages FROM BevCountB GROUP BY beverages ORDER BY sum(counts) ASC LIMIT 10").show()
        // YearC : BevCountC
        println("Lowest Performing Beverages Chain-wide in 2021")
        spark.sql("SELECT beverages FROM BevCountC GROUP BY beverages ORDER BY sum(counts) ASC LIMIT 10").show()
        // BRANCH
        //Join
        println("Branch 2's Least Performing Beverages in 2021")
               spark.sql("SELECT BevBranchC.beverages FROM BevBranchC" +
          " JOIN BevCountC ON BevBranchC.beverages = BevCountC.beverages  WHERE BevBranchC.branches='Branch2'" +
          "GROUP BY BevBranchC.beverages " +
          "ORDER BY sum(BevCountC.counts) ASC LIMIT 10").show()

        /*    spark.sql("SELECT COUNT(bbc.beverages), bbc.beverages FROM BevBranchC bbc " +
              "INNER JOIN BevCountC bcc ON bbc.beverages == bcc.beverages WHERE bbc.branches='Branch9' GROUP BY bbc.beverages").show(60)*/
        // TESTING QUERIES
      /*  spark.sql("SELECT BevBranchC.beverages, sum(BevCountC.counts) FROM BevBranchC " +
          "JOIN BevCountC ON BevBranchC.beverages = BevCountC.beverages WHERE bbc.branches='Branch6' " +
          "GROUP BY BevBranchC.beverages ORDER BY sum(BevCountC.counts) ASC LIMIT 10").show()*/



      }


    }

  }
}

