import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONException;
import org.json.JSONObject;
public class ProcessResumeReducer
extends Reducer<Text, Text, Text, Text> {
	  JSONObject jsonObject;
	 
	  @Override
	  public void reduce(Text key, Iterable<Text> values,
	      Context context)
	      throws IOException, InterruptedException {
	
	  
	  for (Text str : values) {
		  String value1 = str.toString();
		  System.out.println("jsonobject:YES + " + value1);
	try {
		jsonObject = new JSONObject(value1);
	} catch (JSONException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	  
	  String sar="";
	  String[] array = {"Name".replaceAll(" ","").toLowerCase()
			  ,"Email".replaceAll(" ","").toLowerCase()
			  ,"Phone No".replaceAll(" ","").toLowerCase()
			  ,"Alternate Phone".replaceAll(" ","").toLowerCase()
			  ,"Years of Exp".replaceAll(" ","").toLowerCase()
			  ,"Location".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Name".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Description".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Role/Responsibility".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Environment".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Awards".replaceAll(" ","").toLowerCase()
			  ,"Project 1 Start_date".replaceAll(" ","").toLowerCase()
			  ,"Project 1 End Date".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Name".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Description".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Role/Responsibility".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Environment".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Awards".replaceAll(" ","").toLowerCase()
			  ,"Project 2 Start_date".replaceAll(" ","").toLowerCase()
			  ,"Project 2 End Date".replaceAll(" ","").toLowerCase()
			  
			  ,"Under_grad degree".replaceAll(" ","").toLowerCase()
			  ,"Under_grad University".replaceAll(" ","").toLowerCase()
			  ,"Under_grad_passing_year".replaceAll(" ","").toLowerCase()
			  ,"Under_grad CGPA".replaceAll(" ","").toLowerCase()
			  ,"Post_grad_degree".toLowerCase()
			  ,"Post_grad_University".toLowerCase()
			  ,"Post_grad_passing_year".toLowerCase()
			  ,"Post_grad CGPA".replaceAll(" ","").toLowerCase()
};
	  
System.out.println("jsonobject:"+jsonObject);
	  for(int i=0;i<28;i++)
	  {
		  if(!sar.equals(""))
		  {
		  try{
			  sar=sar+","+jsonObject.getString(array[i]);
		  }
		  catch (JSONException e) {
			sar=sar+","+"null";
		  }
		  }
		  else
		  {
			  try{
				  sar=sar+""+jsonObject.getString(array[i]);
			  }
			  catch (JSONException e) {
				sar=sar+""+"null";
			  }  
		  }
	  }
	  String x="";
    context.write(new Text(x), new Text(sar));
  }
	  }
}