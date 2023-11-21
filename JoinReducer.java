import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String title = "", year = "", genre = "";
        Set<String> actors = new HashSet<>();
        Set<String> directors = new HashSet<>();

        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            switch (parts[0]) {
                case "titleBasics":
                    title = parts[2];
                    year = parts[3];
                    genre = parts[4];
                    break;
                case "actor":
                    actors.add(parts[2]);
                    break;
                case "director":
                    directors.add(parts[1]);
                    break;
            }
        }

        for (String actor : actors) {
            if (directors.contains(actor)) {
                context.write(key, new Text(title + "\t" + year + "\t" + genre + "\t" + actor));
            }
        }
    }
}
