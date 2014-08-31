import os
import sys
import subprocess

def stop_err( msg ):
    sys.stderr.write( "%s\n" % msg )
    sys.exit()

def main():
    jar_path = sys.argv[1]
    input_filename = sys.argv[2]
    output_filename = sys.argv[3]   
    slots = sys.argv[4]
    hdfs_in = sys.argv[5] + '_' + str(os.getpid()) + '_in'
    hdfs_out= sys.argv[5] + '_' + str(os.getpid()) + '_out'
    readlen = sys.argv[6]
    k = sys.argv[7]
    
    rmdir_cmd = 'hadoop fs -rmr %s' % (hdfs_in)
    upload_cmd = 'hadoop fs -put %s %s' % (input_filename, hdfs_in)
    CloudRS_cmd = 'hadoop jar %s/ReadStackCorrector.jar -in %s -out %s -slots %s -javaopts -Xmx960m 2>&1' % (jar_path, hdfs_in, hdfs_out, slots)
    CloudBrush_cmd = 'hadoop jar %s/CloudBrush.jar -reads %s -asm %s_brush -k %s -readlen %s -slots %s -javaopts -Xmx960m 2>&1' % (jar_path, hdfs_out, hdfs_out, k, readlen, slots)
    download_cmd = 'hadoop fs -cat %s_brush/* > %s' % (hdfs_out, output_filename)
    
    rmdir_cmd1 = 'hadoop fs -rmr %s' % (hdfs_in)
    rmdir_cmd2 = 'hadoop fs -rmr %s' % (hdfs_out)
    rmdir_cmd3 = 'hadoop fs -rmr %s.tmp' % (hdfs_out)
    rmdir_cmd4 = 'hadoop fs -rmr %s_file' % (hdfs_out)
    rmdir_cmd5 = 'hadoop fs -rmr %s_trim' % (hdfs_out)
    rmdir_cmd6 = 'hadoop fs -rmr %s_brush' % (hdfs_out)
    rmdir_cmd7 = 'hadoop fs -rmr %s_brush.tmp' % (hdfs_out)

    # rm dir in hdfs
    proc = subprocess.call( args=rmdir_cmd, shell=True, stderr=subprocess.PIPE )
    print 'remove directory...[%s]' % (rmdir_cmd)
    # upload file in hdfs
    proc = subprocess.call( args=upload_cmd, shell=True )
    print 'upload file...[%s]' % (upload_cmd)
    # execute CloudRS
    proc = subprocess.call( args=CloudRS_cmd, shell=True )
    print 'perform CloudRS...[%s]' % (CloudRS_cmd)
    # execute CloudBrush
    proc = subprocess.call( args=CloudBrush_cmd, shell=True )
    print 'perform CloudBrush...[%s]' % (CloudBrush_cmd)
    # download output from hdfs
    proc = subprocess.call( args=download_cmd, shell=True, stderr=subprocess.PIPE )
    print 'download output...[%s]' % (download_cmd)
    # remove directory in hdfs
    proc = subprocess.call( args=rmdir_cmd1, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd2, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd3, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd4, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd5, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd6, shell=True, stderr=subprocess.PIPE )
    proc = subprocess.call( args=rmdir_cmd7, shell=True, stderr=subprocess.PIPE )

if __name__ == "__main__": main()

