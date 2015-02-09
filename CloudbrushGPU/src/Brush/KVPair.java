package Brush;

public class KVPair
{
	private String _key;
	private String[] _value;
	
	private int _size = 0;
	
	public KVPair()
	{
		
	}
	
	public KVPair( int size )
	{
		_value = new String[size + 1];
		
	}
	
	public synchronized void addValue(String val)
	{
		_value[_size] = val;
		_size++;
	}
	public synchronized void setKey(String k)
	{
		_key = k;
	}
	public String getKey()
	{
		return _key;
	}
	
	public String[] getValues()
	{
		return _value;
	}
}
