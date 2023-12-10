import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
