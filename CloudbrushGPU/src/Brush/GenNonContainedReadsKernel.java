package Brush;

import java.io.IOException;
import org.trifort.rootbeer.runtime.RootbeerGpu;
//import Brush.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.trifort.rootbeer.runtime.Kernel;

public class GenNonContainedReadsKernel implements Kernel
{
	String m_msg;
	String map_key;
//	Node map_value = new Node();
//	Node node;
//	public List< String, List< String >> fields = new ArrayList< String, List< String >>();
	String nodeId;
	KVPair[] fields; 

	public GenNonContainedReadsKernel()
	{
		
	}
	public GenNonContainedReadsKernel(String msg)
	{
		m_msg = msg;
//		node = n;
	}

	@Override
	public void gpuMethod()
	{
    int blockSize = RootbeerGpu.getBlockDimx();
    int gridSize = (int) RootbeerGpu.getGridDimx();
    int block_idxx = RootbeerGpu.getBlockIdxx();
    int thread_idxx = RootbeerGpu.getThreadIdxx();
    System.out.println("blockSize: " + blockSize);
    System.out.println("gridSize: " + gridSize);
    System.out.println("block_idxx: " + block_idxx);
//    System.out.println("items(M): " + m_M);
    System.out.println("thread_idxx: " + thread_idxx);
//    System.out.println("itemsPerBlock: " + itemsPerBlock);
//		Node nodes = new Node();

		
//		String msg = iter.next().toString();

		String[] vals = m_msg.split( "\t" );
		fields = new KVPair[vals.length];

		if ( vals[ 2 ].equals( Node.NODEMSG ) )
		{
//			node = new Node( vals[ 1 ] );
//			node.setNodeId( vals[1] );
			nodeId = vals[1];
			// node.fromNodeMsg(msg);
			try
			{
				//node.
				parseNodeMsg( vals, 2 );
			}
			catch ( IOException e )
			{
				// TODO Auto-generated catch block
				System.err.println(e.getCause().toString());
				e.printStackTrace();
			}
//			nodes_fr.put( node.getNodeId() + "|" + vals[ 0 ], node );
			map_key =  vals[1] + "|" + vals[ 0 ];
//			map_value = node;
//			if ( vals[ 0 ].equals( "f" ) )
//			{
				// nodes.put(node.getNodeId(), node);
//				nodes.add( node );
//			} // if (vals[0].equals("f"))
		} // for ( String key : Node.edgetypes )

	}
	
	public String[] mapGet(String key)
	{
		for (int i =0; i<fields.length; i++)
		{
			if (key.equals( fields[i].getKey() ))
			{
				return fields[i].getValues();
			}
		}
		return null;
	}
	
	public void parseNodeMsg( String[] items, int offset ) throws IOException
	{
		if ( !items[ offset ].equals( Node.NODEMSG ) )
		{
			throw new IOException( "Unknown code: " + items[ offset ] );
		}
		
		int fieldSize = 0;
		int lSize = 0;

//		List< String > l = null;
		String[] l = null;


		offset++;

		while ( offset < items.length )
		{
			if ( items[ offset ].charAt( 0 ) == '*' )
			{
				String type = items[ offset ].substring( 1 );
				l = mapGet( type );
				lSize = l.length;

				if ( lSize == 0 )
				{
					//l = new String[items.length];//new ArrayList< String >();
					fields[fieldSize].setKey( type );
					//fields[fieldSize].
//					fields.put( type, l );
					fieldSize++;
					
				}
			}
			else if ( lSize != 0 )
			{
//				l.add( items[ offset ] );
				l[lSize] = items[offset];
				lSize++;
			}

			offset++;
		}
	}
	
//Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) 
	{
		new GenNonContainedReadsKernel(null);

	}
	

}
