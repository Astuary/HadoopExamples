1. Write Mapper, Reducer and Driver logic.
Files are in 'src' folder.

2. Dataset is in 'data' folder.

3. Get classpath from
G:\Hadoop\hadoop-2.10.0> bin\hadoop classpath

4. Compile java files
G:\Hadoop\LastFM\src> javac *.java -cp "G:\Hadoop\hadoop-2.10.0\etc\hadoop;G:\Hadoop\hadoop-2.10.0\share\hadoop\common\lib\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\common\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\hdfs;G:\Hadoop\hadoop-2.10.0\share\hadoop\hdfs\lib\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\hdfs\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\yarn;G:\Hadoop\hadoop-2.10.0\share\hadoop\yarn\lib\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\yarn\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\mapreduce\lib\*;G:\Hadoop\hadoop-2.10.0\share\hadoop\mapreduce\*"

5. Make jar file
G:\Hadoop\LastFM\src>jar -cvf UniqueLastFMListeners.jar *.class

6. Put dataset on cloud
G:\Hadoop\hadoop-2.10.0> hadoop dfs -put <path to dataset on local machine G:\Hadoop\LastFM\data\lastfm_sample.txt> <path to where you want to put dataset on cloud server(in our case: root) \>

7. Execute
G:\Hadoop\hadoop-2.10.0> hadoop jar <path to jar file G:\Hadoop\LastFM\jar\UniqueLastFMListeners.jar> <Main Class name: UniqueListeners> <Input Directory: \lastfm_sample.txt> <Output Directory: \ListenersReport>