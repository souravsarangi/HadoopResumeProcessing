import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;
public class ProcessResumeMapper
  extends Mapper<Text, Text, Text, Text> {
  
  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
	 
	String line = value.toString(); 	
    String[] parts = line.split("\n");
    JSONObject obj = new JSONObject();
   

    String filePathString="sar";
    for(int i=0;i<parts.length;i++)
    {
    	//System.out.println("Yes3");
    	String[] parts1 = parts[i].split(":");
	//	System.out.println("YES4:"+parts1.length);
//
    	if(parts1.length>=2){
    		//System.out.println("Yes5");
    		//System.out.println(parts1);
    		String type  = (parts1[0].replaceAll(" ","")).toLowerCase();
    		if(type.equals("name")){      
    			filePathString=parts[1]; 
    		}
    		String values= parts1[1];
    		//System.out.println("Yes6");
    		//dict.put(type, values);
    		 try {
				obj.put(type, values);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
    String req= obj.toString();;
    context.write(new Text(filePathString),new Text(req));
    }
}