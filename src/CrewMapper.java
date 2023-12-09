import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class CrewMapper extends Mapper<LongWritable, Text, Text, Text> {

    // EMITS seperate for each director
    // @Override
    // public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    //     String[] fields = value.toString().split("\t");
        
    //     if (fields.length >= 2) {
    //         String titleId = fields[0];
    //         String[] directors = fields[1].split(",");
            
    //         StringBuilder crewInfo = new StringBuilder("crew," + titleId + ","); 
    //         for (String director : directors) {
    //             if (!"\\N".equals(director.trim()))
    //                 context.write(new Text(titleId), new Text("crew," + titleId + "," + director));
    //         }

    //     }
    // }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length >= 2) {
            String titleId = fields[0];
            String[] directors = fields[1].split(",");

            StringBuilder crewInfo = new StringBuilder("crew," + titleId + ","); 
            for (String director : directors) {
                context.append(director).append(",");
            }

            if(crewInfo.length() > 0) {
                crewInfo.deleteCharAt(crewInfo.length() - 1);
            }

            context.write(new Text(titleId), new Text(crewInfo.toString()));
        }
    }
}
