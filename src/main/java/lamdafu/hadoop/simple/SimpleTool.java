package lamdafu.hadoop.simple;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Doesn't do much but thats the point :)
 * 
 * @author matt
 *
 */
public class SimpleTool extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "SimpleTool");
		job.setJarByClass(SimpleTool.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : -1;
	}
	
    public static void main(String... args) throws Exception {
    	SimpleTool tool = new SimpleTool();
        int res = ToolRunner.run(tool.getConf(), tool, args);
        System.exit(res);
    }

}