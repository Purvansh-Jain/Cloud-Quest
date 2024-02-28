We use Gson as our third-part module.
To run this project, you need first download docWordCount.table and index.table for each kvs workers and put them into workers. This project should work fine within 
Then you need to download a Gson jar file and add it into lib directory.
We have add IntelliJ configuration in the package, so it would be great if you can run in IntelliJ. 
After finishing above, you can start a KVS Master node and two KVS worker, the master need one argument which is its port, the worker need three argument which is its running port and worker name and ip of master. Then you can run ClientServer. For clientServer there is argument that specifies the running KVS master port. 
For the table file, you can download from below link:
https://drive.google.com/drive/folders/1N7JSDWhVcf4TPuiXcL3U6T9_EDVyNsIb?usp=sharing
For Gson jar, you can find in below link and download 2.10.1 or 2.9.* should be fine.
https://central.sonatype.com/artifact/com.google.code.gson/gson/2.10.1/versions
