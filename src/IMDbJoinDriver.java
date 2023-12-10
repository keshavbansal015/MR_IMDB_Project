import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class IMDbJoinDriver {
    public static class MapperActors
            extends Mapper<Object, Text, Text, Text> {

        private final static Text outValue = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] result = str.toString().split(",");
                int n = result.length;

                String titleId = result[0];
                String actorId = result[1];
                String actorName = result[2];

                if (!titleId.equals("\\N") && !actorId.equals("\\N") && !actorName.equals("\\N")) {
                    String newKey = titleId;
                    String newValue = "a" + ";" + titleId + ";" + actorId + ";" + actorName;
                    word.set(newKey);
                    outValue.set(newValue);
                    context.write(word, outValue);
                }
            }
        }
    }

    public static class MapperCrew extends Mapper<Object, Text, Text, Text> {
        private final static Text outValue = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] result = str.toString().split("\t");
                int n = result.length;

                String titleId = result[0];
                String directorsId = result[1];
                if (titleId.equals("tconst")) {
                    continue;
                }
                if (!titleId.equals("\\N") && !directorsId.equals("\\N")) {
                    String newKey = titleId;
                    String newValue = "c" + ";" + titleId + ";" + directorsId;
                    word.set(newKey);
                    outValue.set(newValue);
                    context.write(word, outValue);
                }
            }
        }
    }

    public static class MapperBasics extends Mapper<Object, Text, Text, Text, Text, Text, Text, Text, Text, Text> {
        private final static Text outValue = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String str = itr.nextToken();
                String[] result = str.toString().split("\t");
                int n = result.length;

                String titleId = result[0];
                String titleType = result[1];
                String primaryTitle = result[2];
                String originalTitle = result[3];
                String isAdult = result[4];
                String startYear = result[5];
                String endYear = result[6];
                String runtimeMinutes = result[7];
                String genres = result[8];
                if (titleId.equals("tconst") || !titleType.equals("movie") || !isYearInRange(startYear, 1931, 1940)
                        || !titleId.equals("\\N") || !titleType.equals("\\N") || !originalTitle.equals("\\N")) {
                    continue;
                }
                String newKey = titleId;
                String newValue = "b" + ";" + titleId + ";" + titleType + ";" + originalTitle + ";" + startYear + ";"
                        + genres;
                word.set(newKey);
                outValue.set(newValue);
                context.write(word, outValue);
            }
        }

        private boolean isYearInRange(String year, int startYear, int endYear) {
            try {
                int yearInt = Integer.parseInt(year);
                return yearInt >= startYear && yearInt <= endYear;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    public static class ImdbReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text outValue = new Text();
        private Text word = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize variables to store actor and director details
            String actorDetails = null;
            String directorDetails = null;
            String titleDetails = null;

            // Iterate over values to separate actor and director information
            for (Text val : values) {
                String firstTwoChars = val.toString().substring(0, 2);
                String value = val.toString();
                if (firstTwoChars.equals("a;")) { // Assuming actor values contain the word 'actor'
                    actorDetails = value;
                } else if (firstTwoChars.equals("c;")) { // Assuming director values contain the word 'director'
                    directorDetails = value;
                } else if (firstTwoChars.equals("b;")){
                    titleDetails = value;
                }
            }
            
            String actorId = actorDetails.split(";")[2];
            String[] directorIds = directorDetails.split(";")[2].split(",");

            for (String directorId: directorIds) {
                if (directorId.equals(actorId)){
                    String newKey = "";
                    String[] titleDetailsSplit = titleDetails.split(";");
                    String newValue = titleDetailsSplit[2]+","+titleDetailsSplit[3]+","+titleDetailsSplit[4]+","+titleDetailsSplit[5]+","+actorDetails.split(";")[3];
                    word.set(newKey);
                    outValue.set(newValue);
                    context.write(word, outValue);
                    break;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println(
                    "Usage: IMDbJoinDriver <input path basics> <input path actors> <input path crew> <output path>");
            System.exit(-1);
        }

        String movieData = args[0];
        String actorData = args[1];
        String crewData = args[2];
        String outData = args[3];

        Path moviePath = new Path(movieData);
        Path actorPath = new Path(actorData);
        Path crewPath = new Path(crewData);
        Path outPath = new Path(outData);


        // from presentation
        Configuration conf = new Configuration();

        int split = 700 * 1024 * 1024; // This is in bytes
        String splitsize = Integer.toString(split);
        conf.set("mapreduce.input.fileinputformat.split.minsize", splitsize);
        conf.set("mapreduce.map.memory.mb", "2048"); // This is in Mb
        conf.set("mapreduce.reduce.memory.mb", "2048");

        Job job = Job.getInstance(conf, "imdb project");
        job.setJarByClass(IMDbJoinDriver.class);
        // job.setNumReduceTasks(2); // Sets the no of Reducer

        MultipleInputs.addInputPath(job, actorPath, TextInputFormat.class, MapperActors.class);
        MultipleInputs.addInputPath(job, moviePath, TextInputFormat.class, MapperBasics.class);
        MultipleInputs.addInputPath(job, crewPath, TextInputFormat.class, MapperCrew.class);

        job.setReducerClass(ImdbReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
        System.exit(0);
    }
}

