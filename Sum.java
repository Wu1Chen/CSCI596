
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Sum {


    public static class SumMapper extends Mapper<Object, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();
        @Override
        protected void map(Object offset, Text rows, Context context) throws IOException, InterruptedException {

            String[] cols = rows.toString().split(",");
			String ss = cols[2].replace('\'', ' ');
			
            if (Float.parseFloat(ss)>=20.0) {
				String sss = cols[3].replace('\'', ' ');
				sss=sss.trim();
				int ii=Integer.parseInt(sss);
				
                context.write(new Text(cols[0]), new IntWritable(ii));
            }

        }

    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        protected void reduce(Text province, Iterable<IntWritable> nursing, Context context)
                throws IOException, InterruptedException {
                int sum = 0;
				for (IntWritable val : nursing) {
					sum += val.get();
				}
				result.set(sum);
				context.write(province, result);

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        for (int i = 0; i < otherArgs.length; i++) {
            System.out.println(otherArgs[i]);
        }
        Job job = Job.getInstance(conf, "Sum");
        job.setJarByClass(Sum.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
