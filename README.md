# CloudQuest Dynamic Search Engine Project

## Overview
CloudQuest is an advanced cloud-based search engine that provides an optimized search experience through adaptive web crawling, indexing, and complex ranking algorithms.

## Prerequisites
- Java Development Kit (JDK)
- IntelliJ IDEA (recommended for ease of setup with provided configuration)
- Gson Library (2.10.1 or 2.9.*)

## Setup Instructions
1. **Download Required Tables**:
   - Use `docWordCount.table` and `index.table` from the `workers` directory.

2. **Gson Library**:
   - Download the Gson jar file from [Sonatype Central](https://central.sonatype.com/artifact/com.google.code.gson/gson/2.10.1/versions) and add it to the `lib` directory.

3. **IntelliJ Configuration**:
   - Use the provided IntelliJ configuration for seamless setup.

4. **Running the Nodes**:
   - Start the KVS Master node with one argument for the port.
   - Start two KVS Workers with three arguments each: the running port, worker name, and IP of the master.
   - Run `ClientServer` with the argument specifying the running KVS master port.

## Additional Information
Ensure your environment path variables are set correctly to run the necessary commands.

## Contributors
- Purvansh Jain
