package FlinkTest;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
//import org.apache.flink.datastream.api.stream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class readCSV {
    String path;
    FileSource source;
    DataStream<String> dtStream;

    public readCSV(StreamExecutionEnvironment env,String path,String sourceName,String firstColumnName){
        this.source = buildFileSource(path);
        DataStream<String> ds =buildDataStrem(env,source,sourceName);
        this.dtStream= ds.filter(line -> !line.startsWith(firstColumnName));
    }

    public static FileSource buildFileSource(String path){
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),new Path(path)).build();
        return fileSource;
    }
    public static DataStream buildDataStrem(StreamExecutionEnvironment env, FileSource fileSource,String sourceName){
        var linesFromSource = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                sourceName
        );
        return (DataStream<String>) linesFromSource;
    }


}
