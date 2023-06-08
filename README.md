# How to work Spark in Scala from zero.


Hello Everyone, today we are going to discuss how to work Spark in Scala.


How to know the version of Spark?

First at all, we need to know which version we are working.

You can get the spark version by using the following command:


```
spark-submit --version

spark-shell --version

spark-sql --version

```


How to use two versions of spark shell?

If you have more versions installed like 1.6 and 2.0

you can init scala 1.6 with the following command
```
spark-shell
```
loads Spark 1.6

and init scala 2.0 by typing
```
spark2-shell
```
loads Spark 2.0

of simply if you want start spark 2.0 you can type
```
SPARK_MAJOR_VERSION=2 spark-shell
```


What is Spark Session?

This is one of the most difficult part of spark when you are starting. Because is not enough
know scala, you need to know how it works spark and use properly.


Spark session is a unified entry point of a spark application from Spark 2.0. 
It provides a way to interact with various spark’s functionality. 
Instead of having a spark context, hive context, SQL context as in previous version, now all of it is encapsulated in a Spark session


How do I create a Spark session?

Creating spark session can be done in many ways

1) Directly using sparksession
2) spark conf → spark Context → spark session
3) spark conf → spark session
4) spark conf →spark context


Sparksession Method

To create SparkSession in Scala or Python, you need to use the builder pattern method builder() and calling getOrCreate() method. If SparkSession already exists it returns otherwise creates a new SparkSession.




```
// Create SparkSession object
import org.apache.spark.sql.SparkSession

  val spark = SparkSession.builder()
      .master("local[1]")
      .appName("MyApp")
      .getOrCreate()
```



scala> spark
res1: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@2bd158ea



Get Existing SparkSession
You can get the existing SparkSession in Scala programmatically using the below example
```
// Get existing SparkSession 
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
print(spark)
```


2) sparkconf →sparksession 


Setting spark conf and then passing it into sparksession)

```
val conf = new SparkConf()
conf.set("spark.app.name","appname")
conf.set("spark.master","local[3]")
val sparkSession = SparkSession
                          .builder()
                          .config(conf)
                          .getOrCreate();
```

3) sparkconf →sparkcontext →Spark Session
```
// If you already have SparkContext stored in `sc`
val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
```

```
// Another example which builds a SparkConf, SparkContext and SparkSession
val conf = new SparkConf().setAppName("sparktest").setMaster("local[2]")
val sc = new SparkContext(conf)
val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
```

4)  sparkconf →sparkcontext

```
val conf = new SparkConf()
                    .setAppName("appName")
                    .setMaster("local[*]")
val sc = new SparkContext(conf)
```



Create DataFrame

```
// Create DataFrame
val data = List(("Scala", 25000), ("Spark", 35000), ("PHP", 21000))
val df = spark.createDataFrame(data)
df.show()
```

```
+-----+-----+
|   _1|   _2|
+-----+-----+
|Scala|25000|
|Spark|35000|
|  PHP|21000|
+-----+-----+
```

Working with Spark SQL
Using SparkSession you can access Spark SQL capabilities in Apache Spark. 
In order to use SQL features first, you need to create a temporary view in Spark.
Once you have a temporary view you can run any ANSI SQL queries using spark.sql() method.
```
// Spark SQL
df.createOrReplaceTempView("sample_table")
val df2 = spark.sql("SELECT _1,_2 FROM sample_table")
df2.show()
```

```
// Create Hive table & query it.  
spark.table("sample_table").write.saveAsTable("sample_hive_table")
val df3 = spark.sql("SELECT _1,_2 FROM sample_hive_table")
df3.show()
```


SparkSession Essensial  Methods

version – Returns Spark version where your application is running, probably the Spark version your cluster is configured with.
conf – Returns the RuntimeConfig object.
builder() – builder() is used to create a new SparkSession, this return SparkSession.Builder
newSession() – Creaetes a new SparkSession.
createDataFrame() – This creates a DataFrame from a collection and an RDD
createDataset() – This creates a Dataset from the collection, DataFrame, and RDD.
emptyDataFrame() – Creates an empty DataFrame.
emptyDataset() – Creates an empty Dataset.
getActiveSession() – Returns an active Spark session for the current thread.
getDefaultSession() – Returns the default SparkSession that is returned by the builder.
read() – Returns an instance of DataFrameReader class, this is used to read records from CSV, Parquet, Avro, and more file formats into DataFrame.
sparkContext() – Returns a SparkContext.
sql(String sql) – Returns a DataFrame after executing the SQL mentioned.
sqlContext() – Returns SQLContext.
stop() – Stop the current SparkContext.
table() – Returns a DataFrame of a table or view.
udf() – Creates a Spark UDF to use it on DataFrame, Dataset, and SQL.

We can access spark context and other contexts using the spark session object

scala> spark.sparkContext
res2: org.apache.spark.SparkContext = org.apache.spark.SparkContext@6803b02d


scala> spark.sqlContext
res3: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@74037f9b

Getting the first value from spark.sql
```
df.first
```

Create empty DataFrame
```
val df = spark.emptyDataFrame
```
Create empty DataFrame with schema (StructType)




SQLContext in spark-shell
You can create an SQLContext in Spark shell by passing a default SparkContext object (sc) as a parameter to the SQLContext constructor.




scala> val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

In 2.0 SQLContext() constructor has been deprecated and recommend to use sqlContext method from SparkSession for example spark.sqlContext


```
val sqlContext = spark.sqlContext
```

```
  val sqlContext:SQLContext = spark.sqlContext

  //read csv with options
  val df = sqlContext.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("src/main/resources/zipcodes.csv")
  df.show()
  df.printSchema()
```
```
  df.createOrReplaceTempView("TAB")
  sqlContext.sql("select * from TAB")
    .show(false)
```


Empty Dataframe with no schema
Here we will create an empty dataframe with does not have any schema/columns. For this we will use emptyDataframe() method. Lets us see an example below.

val df: DataFrame =spark.emptyDataFrame
Empty Dataframe with schema
Here we will create an empty dataframe with schema. We will make use of createDataFrame method for creation of dataframe. Just like emptyDataframe here we will make use of emptyRDD[Row] tocreate an empty rdd . We will also create a strytype schema variable. Let us see an example.

  val schema = new StructType()
    .add("fnm",StringType,false)
    .add("lnm",StringType,false)
  val df: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
  df.printSchema()
  root
  |-- fnm: string (nullable = false)
  |-- lnm: string (nullable = false)
  df.show()
  +---+---+
  |fnm|lnm|
  +---+---+
  +---+---+


How to determine the class of a Scala object (getClass)


Spark Read ORC file
Use Spark DataFrameReader’s orc() method to read ORC file into DataFrame. This supports reading snappy, zlib or no compression, it is not necessary to specify in compression option while reading a ORC file.

Spark 2.x:
spark.read.orc("/tmp/orc/data.orc")
In order to read ORC files from Amazon S3, use the below prefix to the path along with third-party dependencies and credentials.

s3:\\ = > First gen
s3n:\\ => second Gen
s3a:\\ => Third gen

Spark 1.6:

hiveContext.read.orc('tmp/orc/data.orc')

```
if(Boolean_expression) {
   // Statements will execute if the Boolean expression is true
}
```
How to match multiple conditions (patterns) with one case statement
```
val cmd = "stop"
cmd match {
    case "start" | "go" => println("starting")
    case "stop" | "quit" | "exit" => println("stopping")
    case _ => println("doing nothing")
}
```

Scala If-Else-If Ladder Example

```
var number:Int = 85  
if(number>=0 && number<50){  
    println ("fail")  
}  
else if(number>=50 && number<60){  
    println("D Grade")  
}  
else if(number>=60 && number<70){  
    println("C Grade")  
}  
else if(number>=70 && number<80){  
    println("B Grade")  
}  
else if(number>=80 && number<90){  
    println("A Grade")  
}  
else if(number>=90 && number<=100){  
    println("A+ Grade")  
}  
else println ("Invalid")  
```


Merge Multiple Data Frames in Spark

```
      // Approach 1
      val mergeDf = empDf1.union(empDf2).union(empDf3)
      mergeDf.show()

      // Approach 2
      val dfSeq = Seq(empDf1, empDf2, empDf3)
      val mergeSeqDf = dfSeq.reduce(_ union _)
      mergeSeqDf.show()
```

Creating a Sequence in Scala
First see how to create a sequence in Scala. The following syntax is used to create a list in Scala,

Syntax:

1)  Creating an empty sequence,
    var emptySeq: Seq[data_type] = Seq();
2)  Creating an Sequence with defining the data type,
    var mySeq: Seq[data_type] = Seq(element1, element2, ...)
3)  Creating an Sequence without defining the data type,
    var mySeq = Seq(element1, element2, ...)

How to initialize a Sequence with 3 elements
val seq1: Seq[String] = Seq("Plain Donut","Strawberry Donut","Chocolate Donut")
ow to add elements to Sequence using :+

val seq2: Seq[String] = seq1 :+ "Vanilla Donut"
The code below shows how to initialize an empty Sequence.
val emptySeq: Seq[String] = Seq.empty[String]

Need	Method
append 1 item	oldSeq :+ e
append multiple items	oldSeq ++ newSeq
prepend 1 item	e +: oldSeq
prepend multiple items	newSeq ++: oldSeq
Remember that Vector and Seq are immutable, so you can’t modify them. Therefore, during the append or prepend operations, you need to assign the result to a new variable.
```
object MyClass {

    def main(args: Array[String])
    {
         val a = Seq("Apple", "Orange", "Mango")

        val temp = a :+ "Watermelon"

         println(temp)
    }
}
```


```
object MyClass {

    def main(args: Array[String])
    {
         val fruits = Seq("Apple", "Orange", "Mango")

         val vegetables = Seq("Onion","tomato","potato")

        val temp = fruits ++ vegetables

         println(temp)
    }
}
```

https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html

How to delete elements from a list in Scala?
```
        var progLang = List("C++", "Java", "Scala", "Python")
        
        println("Programming Languages: " + progLang)
        
        var newLang = progLang.filter(_<"P")
        
        println("Programming Languages: " + newLang)
```

```
import scala.collection.mutable.ListBuffer


var progLang = ListBuffer("C", "C++", "Java", "Scala", "Python", "JavaScript")
        
        println("Programming Languages: " + progLang)
        
        println("Deleting single element")
        progLang -= "Java"
        println("Programming Languages: " + progLang)
        
        println("Deleting multiple elements")
        progLang -= ("C", "Python")
        println("Programming Languages: " + progLang)
```


Converting a Collection to a String with mkString

Use the mkString method to print a collection as a String. Given a simple collection:
```
val a = Array("apple", "banana", "cherry")
```
you can print the collection elements using mkString:
```
a.mkString(", ")
```
res3: String = apple, banana, cherry


Declaring Array Variables
To use an array in a program, you must declare a variable to reference the array and you must specify the type of array the variable can reference.

The following is the syntax for declaring an array variable.
```
var z:Array[String] = new Array[String](3)
```

or
```
var z = new Array[String](3)
```

Here, z is declared as an array of Strings that may hold up to three elements. Values can be assigned to individual elements or get access to individual elements, it can be done by using commands like the following −

```
z(0) = "Zara"; z(1) = "Nuha"; z(4/2) = "Ayan"
```


```
import scala.collection.mutable.ArrayBuffer
val ab = ArrayBuffer[String]()
ab += "hello"
ab += "world"
ab.toArray
```