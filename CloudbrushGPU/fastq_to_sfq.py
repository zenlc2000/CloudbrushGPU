#Dan Blankenberg
import sys
from galaxy_utils.sequence.fastq import fastqReader
#from galaxy_utils.sequence.fasta import fastaWriter

def main():
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    input_type = sys.argv[3] or 'sanger' #input type should ordinarily be unnecessary
    renum = bool(int(sys.argv[4])) 
    num_reads = None
    fastq_read = None
    #out = fastaWriter( open( output_filename, 'wb' ) )
    out = open(output_filename, "w")
    for num_reads, fastq_read in enumerate( fastqReader( open( input_filename ), format = input_type ) ):
        if not renum:
            out.write( "%s\t%s\t%s\n" % (fastq_read.identifier[1:], fastq_read.sequence, fastq_read.quality))
        else:
            out.write( "%x\t%s\t%s\n" % (num_reads, fastq_read.sequence, fastq_read.quality))

    out.close()
    if num_reads is None:
        print "No valid FASTQ reads could be processed."
    else:
        print "%i FASTQ reads were converted to SFQ." % ( num_reads + 1 )
    
if __name__ == "__main__": main()
