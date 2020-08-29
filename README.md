# HBase

Before we start let's talk about the idea of this project. This is an issue related to E-commerce which is how to know the rate of the customers to recommend in the web site for the users. We tried to do a simple map reduce job contains the following steps are spliting, mapping,shuffling and reducing.
    1. Spliting: extract the data 
    2. mapping: set the value of the extracted data as a key value object inside the memory.
    3. shuffling: compare the keys if it's new append a new key and value else if the key is existed we apped a new value.
    4. Reduce: Count the value of each key


Configuration
______________
1. Download and Install HBase
```https://www.tutorialspoint.com/hbase/hbase_installation.htm```
2. Use HBase in 2 Formats 
    * To Use HBase with a SQL Interface you can use it as  
    
       + Apache Drill ```https://drill.apache.org/docs/querying-hbase/```
       
       + Apache Phoenix ```http://phoenix.apache.org/``` 
       
       + Apache Hive can create an 'external table' using HiveQl
           ```
           CREATE EXTERNAL TABLE tablename (
                id int,
                name String
            )
            ROW FORMAT DELIMITED
            COLLECTION ITEMS TERMINATED BY "#"
            STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
            WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:name")
            TBLPROPERTIES ("hbase.table.name" = "tablename");
           ```
               
    * Integrate HBase with a Programming Language Java, Python, Go, etc...
    

Integrate HBase with Java
___________________________

1. go to Project location
```cd JavaHBase```

2. run this command
```hadoop jar target/casestudy-hadoop-ar-one-0.0.1-SNAPSHOT-jar javahbase.JavaHBase hdfs://localhost:9000/transaction.csv hdfs://localhost:9000/javahbase-0000 hdfs://localhost:9000/results```

Extra Resources about CRUD in HBase using Java
```https://www.bogotobogo.com/Hadoop/BigData_hadoop_HBase_Table_with_Java_API.php```
