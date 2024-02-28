package cis5550.tools;

import cis5550.generic.PersistentTableInfo;
import cis5550.kvs.Row;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * Tool to check if table files are corrupted
 */
public class CheckTable {
    /**
     * Run the program
     *
     * @param args paths to worker directories
     */
    public static void main(String[] args) {
        for (String storageDir : args) {
            File folder = new File(storageDir);
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        String fileName = file.getName();
                        if (fileName.endsWith(".table")) {
                            String tableName = fileName.substring(0, fileName.length() - 6);
                            long filePointer = 0, count = 0;
                            Row lastRow = null;
                            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
                                for (filePointer = randomAccessFile.getFilePointer();
                                     filePointer < randomAccessFile.length();
                                     filePointer = randomAccessFile.getFilePointer()) {
                                    Row row = Row.readFrom(randomAccessFile);
                                    count++;
                                    if (row == null) {
                                        break;
                                    }
                                    lastRow = row;
                                }
                                if (filePointer == randomAccessFile.length()) {
                                    System.out.println("Verified table " + tableName + " with " + count + " rows");
                                } else {
                                    System.out.println("Finished scanning table " + tableName +
                                            " but cannot verify all bytes, file pointer after scanning: " + filePointer +
                                            ", last verified row: " + lastRow);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("Failed to verify table " + tableName + " at file pointer " + filePointer + ", last verified row: " + lastRow);
                            }
                        }
                    }
                }
            }
        }
    }
}
