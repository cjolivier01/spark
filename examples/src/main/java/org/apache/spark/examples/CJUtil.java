package org.apache.spark.examples;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.spark.sql.SparkSession;

import java.io.File;

public class CJUtil {
    public static SparkSession getSession(String appName) {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local[*]")
                .getOrCreate();
        return spark;
    }

    public static void deleteFile(String fileName) {
        File file = new File(fileName);
        if(file.exists()) {
            file.delete();
        }
    }

    public static void upload(String fileName,
                              String bucketName,
                              String newName,
                              Boolean overwrite) {
        if(newName.length() == 0) {
            newName = fileName;
        }
        AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
        File file = new File(fileName);
        if(s3client.doesObjectExist(bucketName, newName)) {
            if(!overwrite) {
                return;
            }
            s3client.deleteObject(bucketName, newName);
        }
        s3client.putObject(bucketName, newName, file);
    }

    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }
}
