import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IMDbJoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println(
                    "Usage: IMDbJoinDriver <input path basics> <input path actors> <input path crew> <output path>");
            System.exit(-1);
        }

        // from presentation
        Configuration conf = new Configuration();
        int split = 700 * 1024 * 1024; // This is in bytes
        String splitsize = Integer.toString(split);
        conf.set("mapreduce.input.fileinputformat.split.minsize", splitsize);
        // conf.set("mapreduce.map.memory.mb", "2048"); // This is in Mb
        // conf.set("mapreduce.reduce.memory.mb", "2048");
        Job job1 = Job.getInstance(conf, "actor-director gig");
        job1.setJarByClass(IMDbJoinDriver.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TitleBasicsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ActorsMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, CrewMapper.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[3] + "inter"));
        job1.waitForCompletion(true);
        System.exit(0);

        // Next configuration
        // Configuration conf = new Configuration();
        // Job job1 = Job.getInstance(conf, "actor-director gig");
        // int split = 700*1024*1024; // This is in bytes
        // String splitsize = Integer.toString(split);
        // conf.set("mapreduce.input.fileinputformat.split.minsize",splitsize
        // );
        // // conf.set("mapreduce.map.memory.mb", "2048"); // This is in Mb
        // // conf.set("mapreduce.reduce.memory.mb", "2048");
        // job1.setJarByClass(IMDbJoinDriver.class);
        // job1.setNumReduceTasks(2); // Sets the no of Reducer
        // MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, TitleBasicsMapper.class);
        // MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, ActorsMapper.class);
        // MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, CrewMapper.class);
        // job1.setReducerClass(JoinReducer.class);
        // job1.setMapOutputKeyClass(Text.class);
        // job1.setMapOutputValueClass(Text.class);
        // job1.setOutputValueClass(IntWritable.class);
        // job1.setOutputKeyClass(Text.class);
        // FileOutputFormat.setOutputPath(job1, new Path(args[3]+"inter"));
        // job1.waitForCompletion(true);
        // System.exit(0);
    }
}

class ActorsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length == 3) {
            String titleId = fields[0];
            String actorId = fields[1];
            String actorName = fields[2];
            
            context.write(new Text(titleId), new Text("actor," + titleId + "," + actorId + "," + actorName));
        }
    }
}

class CrewMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if (fields.length >= 2) {
            String titleId = fields[0];
            String[] directors = fields[1].split(",");

            // Use StringBuilder to build the crew information
            StringBuilder crewInfo = new StringBuilder("crew," + titleId + ",");
            for (String director : directors) {
                crewInfo.append(director).append(" ");
            }

            // Remove the last comma if there are directors
            if (directors.length > 0) {
                crewInfo.deleteCharAt(crewInfo.length() - 1);
            }

            // Write the output
            context.write(new Text(titleId), new Text(crewInfo.toString()));
        }
    }
}

class TitleBasicsMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            // Check if the titleType is "movie" and releaseYear is between 2010 and 2020
            if (titleType.equals("movie") && isYearInRange(releaseYear, 1931, 1940)) {
                // Emitting the title ID as key, and the rest of the information as value
                context.write(new Text(titleId),
                        new Text("titleBasics," + titleId + "," + titleName + "," + releaseYear + "," + genres));
            }
        }
    }

    // Helper method to check if the year is in the specified range
    private boolean isYearInRange(String year, int startYear, int endYear) {
        try {
            int yearInt = Integer.parseInt(year);
            return yearInt >= startYear && yearInt <= endYear;
        } catch (NumberFormatException e) {
            // If the year is not a valid integer, return false
            return false;
        }
    }
}

class JoinReducer extends Reducer<Text, Text, Text, Text> {

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
                    context.write(new Text(key), new Text(title + "," + getDirectorList(directors) + "," + actor + "," + genre + "," + year));
                }
            }
        }
    }

    private String getDirectorList(Set<String> directors) {
        StringBuilder directorList = new StringBuilder();
        for (String director : directors) {
            // seperating with spaces instead of commas
            directorList.append(director).append(" ");
        }
        return directorList.toString().trim();
    }
}
