import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IMDbJoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: IMDbJoinDriver <input path basics> <input path actors> <input path crew> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "IMDb Join");
        job.setJarByClass(IMDbJoinDriver.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TitleBasicsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ActorsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CrewMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
