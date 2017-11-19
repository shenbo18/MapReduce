package bo.friendsrecommend;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendsRecommend extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FriendsRecommend(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "FriendsRecommend");
      job.setJarByClass(FriendsRecommend.class);
      
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(NewArrayWritable.class);
      
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
    

      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, IntWritable, NewArrayWritable> {
      private final static IntWritable ONE = new IntWritable(1); //Indicator of direct friendship
      private final static IntWritable ZERO = new IntWritable(0);
      //private Text word = new Text(); */

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	 // Split the input line as User and Friends	
    	 String[] dataValues = value.toString().split("\t");
    	 if (dataValues.length < 2) return;

    	 int user = Integer.parseInt(dataValues[0]);
    	 IntWritable USER = new IntWritable(user);
    	 String[] friends = dataValues[1].split(",");
    	 int[] friendsList = new int[friends.length];
    	 
    	 // Split the Friends list by comma
    	 for (int i = 0; i < friends.length;i++){
    		 int friend = Integer.parseInt(friends[i]);
    		 friendsList[i] = friend;
    		 IntWritable FRIEND = new IntWritable(friend);
    		 NewArrayWritable tuple1 = new NewArrayWritable();
    		 NewArrayWritable tuple2 = new NewArrayWritable();
    		 IntWritable[] array1 = {FRIEND,ZERO};
    		 IntWritable[] array2 = {USER,ZERO};
    		 tuple1.set(array1);
    		 tuple2.set(array2);
    		 context.write(USER, tuple1);
    		 context.write(FRIEND,tuple2);

    	 }
    	 
    	 
    	// Create Pairs shared user as mutual friend
    	 for (int friend1: friendsList) {
    		 IntWritable FRIEND1 = new IntWritable(friend1);
    		 for(int friend2: friendsList){
    			 IntWritable FRIEND2 = new IntWritable(friend2);
    			 if (friend1 !=friend2) {
    				 NewArrayWritable tuple = new NewArrayWritable();
    				 IntWritable[] array = {FRIEND2, ONE};
    				 tuple.set(array);
    				 context.write(FRIEND1, tuple);
    			 }
    		 }
    	 }

      }
   }

   public static class Reduce extends Reducer<IntWritable, NewArrayWritable, IntWritable, Text> {
      @Override
      public void reduce(IntWritable key, Iterable<NewArrayWritable> values, Context context)
              throws IOException, InterruptedException {
    	 // Use a hash table to store candidate recommendations, and number of mutual friends
    	 HashMap<IntWritable,Integer> counts = new HashMap<IntWritable,Integer>();
//    	 //Use a hash table to store direct friends
    	 HashMap<IntWritable,Integer> friends = new HashMap<IntWritable,Integer>();
    	 for (NewArrayWritable val: values){
    		 IntWritable[] aa = (IntWritable[]) val.toArray();
    		 IntWritable candidate = aa[0];
    		 IntWritable indicate = aa[1]; 
    		 
    		 // add to friend list if indicate is zero
    		 if (indicate.get() == 0){
    			 friends.put(candidate, 1);
    		 } 
    		 
    		 // keep counting
    		 if(counts.containsKey(candidate)){
    			 counts.put(candidate, counts.get(candidate)+1);
    		 } else {
    			 counts.put(candidate, 1);
    		 }    		
    	 }

		 // remove candidates that are friends
		 for (IntWritable friend: friends.keySet()) {
			 counts.remove(friend);
		 }

		 // convert counts into treemap in order to sort
		 IntWritableComparator comp = new IntWritableComparator(counts);
		 TreeMap<IntWritable, Integer> sorted = new TreeMap<IntWritable, Integer>(comp);
		 sorted.putAll(counts);

		 // output 10 recommended friends
		 int i = 0;
		 StringBuilder sb = new StringBuilder();
		 for (Entry<IntWritable, Integer> entry : sorted.entrySet()) {
			 sb.append(Integer.toString(entry.getKey().get()));
			 if (i == sorted.size() - 1 || i == 9) {
				 break;
			 } else {
				 sb.append(",");
				 ++i;
			 }
		 }
		 Text list = new Text(sb.toString());
		 context.write(key, list);
		 
      }
   
	 class IntWritableComparator implements Comparator<IntWritable>{
		 
		 HashMap<IntWritable,Integer> counts;
		 
		 public IntWritableComparator(HashMap<IntWritable,Integer> counts){
			 this.counts = counts;
		 }
		 
		 
		 public int compare(IntWritable a, IntWritable b) {
			 if (counts.get(a) < counts.get(b)){
				 return 1;
			 } else if (counts.get(a) == counts.get(b)){
				 if(a.get() < b.get()) {
					 return -1;
				 } else {
					 return 1;
				 }
			 } else {
				 return -1;
			 }
		 }
	 }
   }
   
   public static class NewArrayWritable extends ArrayWritable {
	   public NewArrayWritable() {
		   super(IntWritable.class);
	   }
   }

}