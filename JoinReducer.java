import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

// actors should match the director from the directors tuples

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String title = "", year = "", genre = "";
        Set<String> actors = new HashSet<>();
        Set<String> directors = new HashSet<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
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

        if (!actors.isEmpty() && !directors.isEmpty()) {
            for (String actor : actors) {
                if (directors.contains(actor)) {
                    // EMITTING final output format as key, value (output title, director1 director2, actor, genre, year)
                    context.write(new Text(key), new Text(title + "," + actor + "," + genre + "," + year));
                }
            }
        }
    }

    // private String getDirectorList(Set<String> directors) {
    //     StringBuilder directorList = new StringBuilder();
    //     for (String director : directors) {
    //         // seperating with spaces instead of commas
    //         directorList.append(director).append(" ");
    //     }
    //     return directorList.toString().trim();
    // }
}
