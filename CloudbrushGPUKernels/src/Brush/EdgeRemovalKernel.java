package Brush;

import org.trifort.rootbeer.runtime.Kernel;

public class EdgeRemovalKernel implements Kernel {
	
	private String r_edge;
	public String key;
	public String value;
	public static final String KILLLINKMSG       = "L";
	
	public EdgeRemovalKernel(String edge)
	{
		this.r_edge = edge;
	}

	@Override
	public void gpuMethod() {
		// TODO Auto-generated method stub
		String [] vals = r_edge.split("\\|");
		String id    = vals[0];
		String dir   = vals[1];
		String dead     = vals[2];
		int oval = Integer.parseInt(vals[3]);

		//output.collect(new Text(id),
			//	new Text(Node.KILLLINKMSG + "\t" + dir + "\t" + dead+ "\t" + oval));
		this.key = id;
		this.value = KILLLINKMSG + "\t" + dir + "\t" + dead+ "\t" + oval;
	}

	// Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) {
		new EdgeRemovalKernel(null);
		// TODO Auto-generated method stub

	}

}
