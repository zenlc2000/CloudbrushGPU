package brush;

//import java.io.IOException;

import org.trifort.rootbeer.runtime.Kernel;

public class VerifyOverlapGPU implements Kernel {

	private String[] edges;
	private String key;
	private String OVALMSG;
	private String nodeId;
	private String str_raw;
	//private String con;
	public String[] outKey;
	public String[] outValue;

	public VerifyOverlapGPU(String[] input, String oval, String id, String raw, String k) {
		this.edges = input;
		this.OVALMSG = oval;
		this.nodeId = id;
		this.str_raw = raw;
		this.key = k;
		this.outKey = new String[this.edges.length];
		this.outValue = new String[this.edges.length];
	}



	@Override
	public void gpuMethod() {
		for (int i = 0; i < this.edges.length; i++) {
			String[] vals = this.edges[i].split("!");
			String edge_id = vals[0];
			String oval_size = vals[1];
			String con = flip_link(this.key); 


			// build <key, value> return for the output.collect(k,v)

			this.outKey[i] = edge_id; // <key, >								edge_id

			this.outValue[i] = this.OVALMSG + "\t" + this.nodeId		// < ,value>
					+ "\t" + this.str_raw + "\t" + con
					+ "\t" + oval_size;

		}


	}

	private String flip_link(String link)
	{
		if (link.equals("ff")) { return "rr"; }
		//		if (link.equals("fr")) { return "fr"; }
		//		if (link.equals("rf")) { return "rf"; }
		if (link.equals("rr")) { return "ff"; }
		else { return link; }
	}

	// Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) {
		new VerifyOverlapGPU(null, null, null, null, null);
	}

}
