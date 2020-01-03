1. Compile the Mapper, Reducer and Driver java files
javac *.java -cp <output of bin\hadoop classpath>

2. Create a jar file
jar -cvf <jarname>.jar *.class

3. Put the input dataset in the root directory 
hadoop dfs -put <path to dataset> /

4. Execute the jar file
hadoop jar <path to jar file> <main class name with package hierarchy> <input dataset path on cloud> <output path where you want your output to be>

5. See results at
http://localhost:50070/explorer.html#/
