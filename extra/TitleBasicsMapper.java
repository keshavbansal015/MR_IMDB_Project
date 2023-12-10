import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

import javax.naming.Context;

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
