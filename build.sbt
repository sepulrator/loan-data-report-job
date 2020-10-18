
name := "loan-data-report-job"

version := "0.7"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// emr settings
sparkClusterName := "vngrs-challenge-cluster"

sparkAwsRegion := "us-west-2"

sparkInstanceCount := 3

sparkS3JarFolder := "s3://emr-spark-jobs-bucket/loan-data-report-job/"
sparkS3LogUri := Some("s3://emr-cluster-log-bucket/vngrs-challenge-cluster")

sparkMasterType := "m4.large"
sparkCoreType := "m4.large"

sparkEmrRelease := "emr-6.1.0"

sparkInstanceKeyName := Some("emr-ec2-keypair")

sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
