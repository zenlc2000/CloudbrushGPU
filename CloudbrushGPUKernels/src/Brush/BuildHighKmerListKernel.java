package Brush;

import org.trifort.rootbeer.runtime.Kernel;
import java.util.Map;
import java.util.HashMap;

public class BuildHighKmerListKernel implements Kernel {

	String node_str;
	int index;
	int K;

	static Map< String, String > str2dna_ = initializeSTR2DNA();
	static String[] dnachars = { "A", "C", "G", "T" };

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
			String window = str2dna(window_tmp); // TODO figure this out
			//		output.collect(new Text(window), new IntWritable((int)node.cov()));
		} // if window_tmp
		else 
		{
			String window_r = str2dna(window_tmp_r);	// TODO figure this out
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
	
  // converts strings like A, GA, TAT, ACGT to compressed DNA codes
	// (A,B,C,...,HA,HB)
	private static Map< String, String > initializeSTR2DNA()
	{
		int num = 0;
		int asciibase = 'A';

		Map< String, String > retval = new HashMap< String, String >();

		for ( int xi = 0; xi < dnachars.length; xi++ )
		{
			retval.put( dnachars[ xi ],
					Character.toString( (char) ( num + asciibase ) ) );

			num++;

			for ( int yi = 0; yi < dnachars.length; yi++ )
			{
				retval.put( dnachars[ xi ] + dnachars[ yi ],
						Character.toString( (char) ( num + asciibase ) ) );
				num++;
			}
		}

		for ( int xi = 0; xi < dnachars.length; xi++ )
		{
			for ( int yi = 0; yi < dnachars.length; yi++ )
			{
				String m = retval.get( dnachars[ xi ] + dnachars[ yi ] );

				for ( int zi = 0; zi < dnachars.length; zi++ )
				{
					retval.put( dnachars[ xi ] + dnachars[ yi ] + dnachars[ zi ], m
							+ retval.get( dnachars[ zi ] ) );

					for ( int wi = 0; wi < dnachars.length; wi++ )
					{
						retval.put( dnachars[ xi ] + dnachars[ yi ] + dnachars[ zi ]
								+ dnachars[ wi ],
								m + retval.get( dnachars[ zi ] + dnachars[ wi ] ) );
					}
				}
			}
		}

		return retval;
	}


	public static String str2dna( String seq )
	{
		StringBuffer sb = new StringBuffer();

		int l = seq.length();

		int offset = 0;

		while ( offset < l )
		{
			int r = l - offset;

			if ( r >= 4 )
			{
				sb.append( str2dna_.get( seq.substring( offset, offset + 4 ) ) );
				offset += 4;
			}
			else
			{
				sb.append( str2dna_.get( seq.substring( offset, offset + r ) ) );
				offset += r;
			}
		}

		return sb.toString();
	}


	//Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) {
		new BuildHighKmerListKernel(null, 0, 0);

	}

}
