# Loan Data Report Job

Loan reports are generated using Spark DataFrame API. 
[Sbt Lighter Plugin](https://github.com/pishen/sbt-lighter) is used for management of spark job operations.


#### Build && Run Using Sbt Lighter Plugin
This command;
    * creates fat jar under target directory 
    * uploads jar to s3
    * creates auto-terminated emr cluster with specified settings in build.sbt with initial spark job execution step
```bash  
> sparkSubmitMain com.vngrs.challenge.loan.report.LoanDataReportJob
```
Key Pair,Emr Roles should created beforehand

#### Build && Run Manuel Steps

* Generate Fat Jar Using "sbt assembly"
* Upload generated under target/scala-2.11/loan-data-report-job-assembly-${VERSION}.jar to s3://emr-spark-jobs-bucket/loan-data-report-job/loan-data-report-job-assembly-${VERSION}.jar
* While creating emr cluster(size = 3) select jar from s3 as a initial step and pass "--class com.vngrs.challenge.loan.report.LoanDataReportJob" as spark argument

#### S3 Buckets Info
* Uploaded jars keep under s3://emr-spark-jobs-bucket/loan-data-report-job
* Loan data csv file is stored in s3://loan-data-bucket/2019/*
* Schema of csv file is uploaded to s3://loan-data-bucket/schema/schema.csv
* Reports are generated under s3://loan-data-bucket/report_one and s3://loan-data-bucket/report_two 

#### Notes 
* Compile project with Scala 2.11 
* Minimum emr worker instance should be 2 since there is more memory to process the file
* Using sbt lighter plugin is optional. All setups can be configured via aws console
* After each changes increment version in build.sbt
* Consider last created emr cluster "vngrs-challenge-cluster"  