package Brush;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import edu.syr.pcpratts.rootbeer.runtime.Kernel;

public class VerifyOverlapGPU implements Kernel {

	private Node node;
	private OutputCollector<Text, Text> output;

	public VerifyOverlapGPU(OutputCollector<Text, Text> o, Node n) {
		output = o;
		node = n;
	}

	@Override
	public void gpuMethod() {
		// TODO Auto-generated method stub
		for (String key : Node.edgetypes) {
			// / modify ... 01/15
			// String key = "r" + adj;
			try {
				List<String> edges = node.getEdges(key);
				if (edges != null) {
					for (int i = 0; i < edges.size(); i++) {
						String[] vals = edges.get(i).split("!");
						String edge_id = vals[0];
						String oval_size = vals[1];
						// String con = Node.flip_dir(adj) + "f";
						String con = Node.flip_link(key);

						output.collect(new Text(edge_id),
								new Text(Node.OVALMSG + "\t" + node.getNodeId()
										+ "\t" + node.str_raw() + "\t" + con
										+ "\t" + oval_size));

						// \\// emit reverse edge
						// output.collect(new Text(edge_id), new
						// Text(Node.OVALMSG + "\t" + node.getNodeId() + "\t" +
						// node.str_raw() + "\t" + key + "\t" + oval_size));
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}





//import edu.syr.pcpratts.rootbeer.runtime.Kernel;
//
//public class ArraySum implements Kernel {
//  
//  private int[] source; 
//  private int[] ret; 
//  private int index;
//  
//  public ArraySum (int[] src, int[] dst, int i){
//    source = src; ret = dst; index = i;
//  }
//  
//  public void gpuMethod(){
//    int sum = 0;
//    for(int i = 0; i < source.length; ++i){
//      sum += source[i];
//    }
//    ret[index] = sum;
//  }
//}
