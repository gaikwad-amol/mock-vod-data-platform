#!/bin/bash

# A script to download specified Maven artifacts and all their dependencies
# into a local directory named 'jars'.
#
# Prerequisites:
# - Apache Maven must be installed and available in your system's PATH.
# - You need an internet connection to reach the Maven Central repository.

set -e # Exit immediately if a command exits with a non-zero status.

# Define the output directory for the JAR files
OUTPUT_DIR="jars"

# Create the output directory if it doesn't exist
echo "Creating output directory: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Create a temporary pom.xml file to define the dependencies
echo "Creating temporary pom.xml file..."
cat <<EOF > temp-pom.xml
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>dependency-downloader</artifactId>
    <version>1.0</version>
    <dependencies>
        <!-- Hadoop AWS Connector -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-aws</artifactId>
            <version>3.3.4</version>
        </dependency>
        <!-- Iceberg Spark Runtime -->
        <dependency>
            <groupId>org.apache.iceberg</groupId>
            <artifactId>iceberg-spark-runtime-3.5_2.12</artifactId>
            <version>1.5.2</version>
        </dependency>
        <!-- Iceberg AWS Bundle -->
        <dependency>
            <groupId>org.apache.iceberg</groupId>
            <artifactId>iceberg-aws-bundle</artifactId>
            <version>1.5.2</version>
        </dependency>
    </dependencies>
</project>
EOF

# Use Maven to download all dependencies defined in the pom.xml
echo "Running Maven to download dependencies..."
mvn -f temp-pom.xml dependency:copy-dependencies -DoutputDirectory="./${OUTPUT_DIR}"

# Clean up the temporary pom.xml file
echo "Cleaning up temporary files..."
rm temp-pom.xml

echo "--------------------------------------------------"
echo "✅ Success! All JARs have been downloaded to the '${OUTPUT_DIR}' directory."
echo "--------------------------------------------------"


