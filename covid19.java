
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

public class covid19{


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
                String[] sum= val.toString().split("\t");
	if (sum[0].equals(key.toString())) {
	hisFriends.add(sum[1]);
	context.write(val, new Text("tag1"));
	}
	if (sum[1].equals(key.toString())) {
	hisFriends.add(sum[0]);
	context.write(val, new Text("tag1"));
	}
	}
	for(int i = 0; i < hisFriends.size(); i++)
	{
	for(int j = 0; j < hisFriends.size(); j++)
	{
	if (hisFriends.elementAt(i).compareTo(hisFriends.elementAt(j)) < 0) {
	Text reduce_key = new Text(hisFriends.elementAt(i)+"\t"+hisFriends.elementAt(j));
	context.write(reduce_key, new Text("tag2"));
		}
	}

        }
    }

	public static class Map2 extends Mapper<Object, Text, Text, Text>
	{

	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
	String[] line = value.toString().split("\t");
		if (line.length == 3) {
		Text map2_key = new Text(line[0]+"\t"+line[1]);
		Text map2_value = new Text(line[2]);
		context.write(map2_key, map2_value);
		}
	}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text>
		{

	protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		boolean isdeg1 = false;
		boolean isdeg2 = false;
		int count = 0;
		for(Text val : values)
		{
	if (val.toString().compareTo("tag1") == 0) {
		isdeg1 = true;
	}
	if (val.toString().compareTo("tag2") == 0) {
	isdeg2 = true;
	count++;
	}
	}
		if ((!isdeg1) && isdeg2) {
		context.write(new Text(String.valueOf(count)),key);
		}
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
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
	FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	if (job1.waitForCompletion(true)) {
		Job job2 = new Job(conf, "Deg2friend");
		job2.setJarByClass(deg2Friend.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
		System.exit(job2.waitForCompletion(true)? 0 : 1);
	}
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}