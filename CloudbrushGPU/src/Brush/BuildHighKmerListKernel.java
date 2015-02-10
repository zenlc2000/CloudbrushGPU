package Brush;

import org.trifort.rootbeer.runtime.Kernel;

public class BuildHighKmerListKernel implements Kernel {

	String node_str;
	int index;
	int K;

	public 	BuildHighKmerListKernel(String node_str, int i, int K)
	{
		this.node_str = node_str;
		this.index = i;
		this.K = K;

	}
	@Override
	public void gpuMethod() {
		String window_tmp = node_str.substring(this.index,   this.index+this.K);
		//String window_r_tmp = Node.rc(node.str().substring(node.len() - K - i, node.len() - i));
		String window_tmp_r = rc(window_tmp);
		if (window_tmp.compareTo(window_tmp_r) < 0) 
		{
			String window = Node.str2dna(window_tmp); // TODO figure this out
			//		output.collect(new Text(window), new IntWritable((int)node.cov()));
		} // if window_tmp
		else 
		{
			String window_r = Node.str2dna(window_tmp_r);	// TODO figure this out
			//		output.collect(new Text(window_r), new IntWritable((int)node.cov()));
		} // else 

	}

	private String rc(String seq) //reverse complement
	{
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--)
		{
			if      (seq.charAt(i) == 'A') { sb.append('T'); }
			else if (seq.charAt(i) == 'T') { sb.append('A'); }
			else if (seq.charAt(i) == 'C') { sb.append('G'); }
			else if (seq.charAt(i) == 'G') { sb.append('C'); }
		}

		return sb.toString();
	}

	//Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) {
		new BuildHighKmerListKernel(null, 0, 0);

	}

}
