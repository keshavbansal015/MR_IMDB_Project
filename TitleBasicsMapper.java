import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TitleBasicsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the line from the TSV file into fields
        String[] fields = value.toString().split("\t");

        // Check if the line contains the expected number of fields (5 fields)
        if (fields.length == 5) {
            // Extracting individual fields
            String titleId = fields[0];
            String titleType = fields[1];
            String titleName = fields[2];
            String releaseYear = fields[3];
            String genres = fields[4];

            // Emitting the title ID as key, and the rest of the information as value
            context.write(new Text(titleId), new Text("titleBasics\t" + titleType + "\t" + titleName + "\t" + releaseYear + "\t" + genres));
        }
    }
}
