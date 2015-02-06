import os
import sys
import time
import subprocess
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.emr.connection import EmrConnection
from boto.emr.step import JarStep
from boto.emr.instance_group import InstanceGroup

def stop_err( msg ):
    sys.stderr.write( "%s\n" % msg )
    sys.exit()

def main():
    aws_access = sys.argv[1]
    aws_secert = sys.argv[2]
    jar_path = sys.argv[3]
    input_filename = sys.argv[4]
    output_filename = sys.argv[5]
    nodes = int(sys.argv[6])
    slots = 7 * nodes
    s3_in = sys.argv[7] + "_" + str(os.getpid()) + "_in"
    s3_out= sys.argv[7] + "_" + str(os.getpid()) + "_out"
    s3_asm= sys.argv[7] + "_" + str(os.getpid()) + "_asm"
    readlen = int(sys.argv[8])
    kmer= int(sys.argv[9])


    # connect to S3
    s3_conn = S3Connection(aws_access, aws_secert)
    mybucket = s3_conn.create_bucket(aws_access.lower())
    mybucket = s3_conn.get_bucket(aws_access.lower(), validate=False)
    print "\nConnection created"
    # upload  data
    k = Key(mybucket)
    k.key = 'ReadStackCorrector.jar'
    k.set_contents_from_filename(jar_path + 'ReadStackCorrector.jar')
    #k.key = 'CloudBrush.jar'
    k.key = 'CloudbrushGPU.jar'
    k.set_contents_from_filename(jar_path + 'CloudbrushGPU-GPU.jar')

    # uploading file parallel
    #k.key = s3_in
    #k.set_contents_from_filename(input_filename)
    print "\nStarting Upload"
    s3_path = 's3://%s/%s' % (aws_access.lower(), s3_in)
    upload_cmd = 'python %s/s3-mp-upload.py %s %s %s %s -f 2>&1' % (jar_path, input_filename, s3_path, aws_access, aws_secert)
    proc = subprocess.call( args=upload_cmd, shell=True )

    #k.key = s3_out
    #k.delete()
    # connect to EMR    InstanceGroup(nodes, 'CORE', 'c1.xlarge', 'ON_DEMAND', 'core-spot@0.4', '0.4')
    emr_conn = EmrConnection(aws_access, aws_secert)
    instance_groups = [
        InstanceGroup(1, 'MASTER', 'm1.medium', 'ON_DEMAND', 'master-spot@0.4', '0.4'),
        InstanceGroup(nodes, 'CORE', 'g2.2xlarge', 'ON_DEMAND', 'core-spot@0.4', '0.4')
        ]

    # perform CloudRS
    step1 = JarStep(name='CloudRS',
                   jar='s3n://%s/ReadStackCorrector.jar' % (aws_access.lower()),
                   step_args = ['-in', 's3n://%s/%s' % (aws_access.lower(), s3_in), '-out', s3_out, '-slots', slots, '-javaopts', '-Xmx960m'])

    # perform CloudBrush
    step2 = JarStep(name='CloudBrush',
                   jar='s3n://%s/CloudbrushGPU-GPU.jar' % (aws_access.lower()),
                   step_args = ['-reads', s3_out, '-asm', s3_asm, '-readlen', readlen, '-k', kmer, '-slots', slots, '-javaopts', '-Xmx960m'])

    # copy from hdfs to S3
    k.key = s3_asm
    step3 = JarStep(name='S3DistCp',
                    jar='/home/hadoop/lib/emr-s3distcp-1.0.jar', #'s3://elasticmapreduce/libs/s3distcp/role/s3distcp.jar',
                    step_args = ['--src', 'hdfs:///user/hadoop/%s' % s3_asm , '--dest', 's3://%s/%s' % (aws_access.lower(), s3_asm), '--groupBy', '.*(part).*'])
    jobid = emr_conn.run_jobflow(name='CloudBrush',
                             log_uri='s3://%s/jobflow_logs' % aws_access.lower(),
                             ami_version='latest',
                             hadoop_version='2.4.0', #'0.20.205'
                             keep_alive=False,
                             visible_to_all_users=True,
                             steps=[step1,step2,step3],
                             instance_groups = instance_groups)

    state = emr_conn.describe_jobflow(jobid).state
    print "job state = ", state
    print "job id = ", jobid
    while state != u'COMPLETED':
        print time.asctime(time.localtime())
        time.sleep(30)
        state = emr_conn.describe_jobflow(jobid).state
        print "job state = ", state
        print "job id = ", jobid
        if state == u'FAILED':
            print 'FAILED!!!!'
            break

    # download file parallel
    #k.key = "%s/part0" % (s3_asm)
    #k.get_contents_to_filename(output_filename)
    if state == u'COMPLETED':
        s3_path  = 's3://%s/%s/part0' % (aws_access.lower(), s3_asm)
        download_cmd = 'python %s/s3-mp-download.py %s %s %s %s -f 2>&1' % (jar_path, s3_path, output_filename, aws_access, aws_secert)
        proc = subprocess.call( args=download_cmd, shell=True )


        # delete file in S3
        k.key = s3_in
        k.delete()
        k.key = "%s/part0" % (s3_asm)
        k.delete()



if __name__ == "__main__": main()

