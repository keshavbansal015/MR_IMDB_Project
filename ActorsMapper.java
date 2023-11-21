import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ActorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 3) {
            String titleId = fields[0];
            String actorId = fields[1];
            String actorName = fields[2];
            context.write(new Text(titleId), new Text("actor\t" + actorId + "\t" + actorName));
        }
    }
}
