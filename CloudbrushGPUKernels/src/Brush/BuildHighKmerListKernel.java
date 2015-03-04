package Brush;

import org.trifort.rootbeer.runtime.Kernel;
import java.util.Map;
import java.util.HashMap;

public class BuildHighKmerListKernel implements Kernel {

	String node_str;
	int index;
	int K;

//	static Map< String, String > str2dna_ = initializeSTR2DNA();
	static String[] dnachars = { "A", "C", "G", "T" };

    static String[] str2dnaKeys = {"A", "AA", "AC", "AG", "AT", "C", "CA", "CC", "CG", "CT", "G", "GA", "GC", "GG", "GT", "T", "TA", "TC", "TG", "TT", "AAA",
            "AAAA", "AAAC", "AAAG", "AAAT", "AAC", "AACA", "AACC", "AACG", "AACT", "AAG", "AAGA", "AAGC", "AAGG", "AAGT", "AAT", "AATA",
            "AATC", "AATG", "AATT", "ACA", "ACAA", "ACAC", "ACAG", "ACAT", "ACC", "ACCA", "ACCC", "ACCG", "ACCT", "ACG", "ACGA", "ACGC",
            "ACGG", "ACGT", "ACT", "ACTA", "ACTC", "ACTG", "ACTT", "AGA", "AGAA", "AGAC", "AGAG", "AGAT", "AGC", "AGCA", "AGCC", "AGCG",
            "AGCT", "AGG", "AGGA", "AGGC", "AGGG", "AGGT", "AGT", "AGTA", "AGTC", "AGTG", "AGTT", "ATA", "ATAA", "ATAC", "ATAG", "ATAT",
            "ATC", "ATCA", "ATCC", "ATCG", "ATCT", "ATG", "ATGA", "ATGC", "ATGG", "ATGT", "ATT", "ATTA", "ATTC", "ATTG", "ATTT", "CAA",
            "CAAA", "CAAC", "CAAG", "CAAT", "CAC", "CACA", "CACC", "CACG", "CACT", "CAG", "CAGA", "CAGC", "CAGG", "CAGT", "CAT", "CATA",
            "CATC", "CATG", "CATT", "CCA", "CCAA", "CCAC", "CCAG", "CCAT", "CCC", "CCCA", "CCCC", "CCCG", "CCCT", "CCG", "CCGA", "CCGC",
            "CCGG", "CCGT", "CCT", "CCTA", "CCTC", "CCTG", "CCTT", "CGA", "CGAA", "CGAC", "CGAG", "CGAT", "CGC", "CGCA", "CGCC", "CGCG",
            "CGCT", "CGG", "CGGA", "CGGC", "CGGG", "CGGT", "CGT", "CGTA", "CGTC", "CGTG", "CGTT", "CTA", "CTAA", "CTAC", "CTAG", "CTAT",
            "CTC", "CTCA", "CTCC", "CTCG", "CTCT", "CTG", "CTGA", "CTGC", "CTGG", "CTGT", "CTT", "CTTA", "CTTC", "CTTG", "CTTT", "GAA",
            "GAAA", "GAAC", "GAAG", "GAAT", "GAC", "GACA", "GACC", "GACG", "GACT", "GAG", "GAGA", "GAGC", "GAGG", "GAGT", "GAT", "GATA",
            "GATC", "GATG", "GATT", "GCA", "GCAA", "GCAC", "GCAG", "GCAT", "GCC", "GCCA", "GCCC", "GCCG", "GCCT", "GCG", "GCGA", "GCGC",
            "GCGG", "GCGT", "GCT", "GCTA", "GCTC", "GCTG", "GCTT", "GGA", "GGAA", "GGAC", "GGAG", "GGAT", "GGC", "GGCA", "GGCC", "GGCG",
            "GGCT", "GGG", "GGGA", "GGGC", "GGGG", "GGGT", "GGT", "GGTA", "GGTC", "GGTG", "GGTT", "GTA", "GTAA", "GTAC", "GTAG", "GTAT",
            "GTC", "GTCA", "GTCC", "GTCG", "GTCT", "GTG", "GTGA", "GTGC", "GTGG", "GTGT", "GTT", "GTTA", "GTTC", "GTTG", "GTTT", "TAA",
            "TAAA", "TAAC", "TAAG", "TAAT", "TAC", "TACA", "TACC", "TACG", "TACT", "TAG", "TAGA", "TAGC", "TAGG", "TAGT", "TAT", "TATA",
            "TATC", "TATG", "TATT", "TCA", "TCAA", "TCAC", "TCAG", "TCAT", "TCC", "TCCA", "TCCC", "TCCG", "TCCT", "TCG", "TCGA", "TCGC",
            "TCGG", "TCGT", "TCT", "TCTA", "TCTC", "TCTG", "TCTT", "TGA", "TGAA", "TGAC", "TGAG", "TGAT", "TGC", "TGCA", "TGCC", "TGCG",
            "TGCT", "TGG", "TGGA", "TGGC", "TGGG", "TGGT", "TGT", "TGTA", "TGTC", "TGTG", "TGTT", "TTA", "TTAA", "TTAC", "TTAG", "TTAT",
            "TTC", "TTCA", "TTCC", "TTCG", "TTCT", "TTG", "TTGA", "TTGC", "TTGG", "TTGT", "TTT", "TTTA", "TTTC", "TTTG", "TTTT"};

    static String[] str2dnaValues = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "BA",
            "BB", "BC", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BK", "BL", "BM", "BN", "BO", "BP", "BQ", "BR", "BS", "BT", "CA", "CB", "CC",
            "CD", "CE", "CF", "CG", "CH", "CI", "CJ", "CK", "CL", "CM", "CN", "CO", "CP", "CQ", "CR", "CS", "CT", "DA", "DB", "DC", "DD", "DE",
            "DF", "DG", "DH", "DI", "DJ", "DK", "DL", "DM", "DN", "DO", "DP", "DQ", "DR", "DS", "DT", "EA", "EB", "EC", "ED", "EE", "EF", "EG",
            "EH", "EI", "EJ", "EK", "EL", "EM", "EN", "EO", "EP", "EQ", "ER", "ES", "ET", "GA", "GB", "GC", "GD", "GE", "GF", "GG", "GH", "GI",
            "GJ", "GK", "GL", "GM", "GN", "GO", "GP", "GQ", "GR", "GS", "GT", "HA", "HB", "HC", "HD", "HE", "HF", "HG", "HH", "HI", "HJ", "HK", "HL",
            "HM", "HN", "HO", "HP", "HQ", "HR", "HS", "HT", "IA", "IB", "IC", "ID", "IE", "IF", "IG", "IH", "II", "IJ", "IK", "IL", "IM", "IN", "IO",
            "IP", "IQ", "IR", "IS", "IT", "JA", "JB", "JC", "JD", "JE", "JF", "JG", "JH", "JI", "JJ", "JK", "JL", "JM", "JN", "JO", "JP", "JQ", "JR",
            "JS", "JT", "LA", "LB", "LC", "LD", "LE", "LF", "LG", "LH", "LI", "LJ", "LK", "LL", "LM", "LN", "LO", "LP", "LQ", "LR", "LS", "LT", "MA",
            "MB", "MC", "MD", "ME", "MF", "MG", "MH", "MI", "MJ", "MK", "ML", "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "NA", "NB", "NC", "ND",
            "NE", "NF", "NG", "NH", "NI", "NJ", "NK", "NL", "NM", "NN", "NO", "NP", "NQ", "NR", "NS", "NT", "OA", "OB", "OC", "OD", "OE", "OF", "OG",
            "OH", "OI", "OJ", "OK", "OL", "OM", "ON", "OO", "OP", "OQ", "OR", "OS", "OT", "QA", "QB", "QC", "QD", "QE", "QF", "QG", "QH", "QI", "QJ",
            "QK", "QL", "QM", "QN", "QO", "QP", "QQ", "QR", "QS", "QT", "RA", "RB", "RC", "RD", "RE", "RF", "RG", "RH", "RI", "RJ", "RK", "RL", "RM",
            "RN", "RO", "RP", "RQ", "RR", "RS", "RT", "SA", "SB", "SC", "SD", "SE", "SF", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SP",
            "SQ", "SR", "SS", "ST", "TA", "TB", "TC", "TD", "TE", "TF", "TG", "TH", "TI", "TJ", "TK", "TL", "TM", "TN", "TO", "TP", "TQ", "TR", "TS", "TT"};


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

    public int findIndex(String key)
    {
        int i;

        System.out.println("Key = " + key);

        for (i = 0; i < str2dnaKeys.length; i++ )
        {
//            System.out.println("Trying: " + str2dnaKeys[i]);
            if ( str2dnaKeys[i].equals(key) )
            {
                return i;
            }
        }
        return i;
    }

    public String str2dna( String seq )
    {
        StringBuffer sb = new StringBuffer();

        int l = seq.length();

        int offset = 0;
        int index = 0;


        while ( offset < l )
        {
            int r = l - offset;

            if ( r >= 4 )
            {
//				System.out.println(seq.substring( offset, offset + 4 ));
//				sb.append( str2dna_.get( seq.substring( offset, offset + 4 ) ) );
                index = findIndex(seq.substring( offset, offset + 4 ));
                sb.append( str2dnaValues[index] );
                offset += 4;
            }
            else
            {
//				System.out.println( seq.substring( offset, offset + r ) );
//				sb.append( str2dna_.get( seq.substring( offset, offset + r ) ) );
                index = findIndex( seq.substring( offset, offset + r ) );
                sb.append( str2dnaValues[index] );
                offset += r;
            }
        }

        return sb.toString();
    }

//	public static String str2dna( String seq )
//	{
//		StringBuffer sb = new StringBuffer();
//
//		int l = seq.length();
//
//		int offset = 0;
//
//		while ( offset < l )
//		{
//			int r = l - offset;
//
//			if ( r >= 4 )
//			{
//				sb.append( str2dna_.get( seq.substring( offset, offset + 4 ) ) );
//				offset += 4;
//			}
//			else
//			{
//				sb.append( str2dna_.get( seq.substring( offset, offset + r ) ) );
//				offset += r;
//			}
//		}
//
//		return sb.toString();
//	}


	//Dummy constructor invocation to keep the constructor within the Rootbeer transformation
	public static void main(String[] args) {
		new BuildHighKmerListKernel(null, 0, 0);

	}

}

/* contents of map created by initializeSTR2DNA()

A, A
AA, B
AC, C
AG, D
AT, E
C, F
CA, G
CC, H
CG, I
CT, J
G, K
GA, L
GC, M
GG, N
GT, O
T, P
TA, Q
TC, R
TG, S
TT, T
AAA, BA
AAAA, BB
AAAC, BC
AAAG, BD
AAAT, BE
AAC, BF
AACA, BG
AACC, BH
AACG, BI
AACT, BJ
AAG, BK
AAGA, BL
AAGC, BM
AAGG, BN
AAGT, BO
AAT, BP
AATA, BQ
AATC, BR
AATG, BS
AATT, BT
ACA, CA
ACAA, CB
ACAC, CC
ACAG, CD
ACAT, CE
ACC, CF
ACCA, CG
ACCC, CH
ACCG, CI
ACCT, CJ
ACG, CK
ACGA, CL
ACGC, CM
ACGG, CN
ACGT, CO
ACT, CP
ACTA, CQ
ACTC, CR
ACTG, CS
ACTT, CT
AGA, DA
AGAA, DB
AGAC, DC
AGAG, DD
AGAT, DE
AGC, DF
AGCA, DG
AGCC, DH
AGCG, DI
AGCT, DJ
AGG, DK
AGGA, DL
AGGC, DM
AGGG, DN
AGGT, DO
AGT, DP
AGTA, DQ
AGTC, DR
AGTG, DS
AGTT, DT
ATA, EA
ATAA, EB
ATAC, EC
ATAG, ED
ATAT, EE
ATC, EF
ATCA, EG
ATCC, EH
ATCG, EI
ATCT, EJ
ATG, EK
ATGA, EL
ATGC, EM
ATGG, EN
ATGT, EO
ATT, EP
ATTA, EQ
ATTC, ER
ATTG, ES
ATTT, ET
CAA, GA
CAAA, GB
CAAC, GC
CAAG, GD
CAAT, GE
CAC, GF
CACA, GG
CACC, GH
CACG, GI
CACT, GJ
CAG, GK
CAGA, GL
CAGC, GM
CAGG, GN
CAGT, GO
CAT, GP
CATA, GQ
CATC, GR
CATG, GS
CATT, GT
CCA, HA
CCAA, HB
CCAC, HC
CCAG, HD
CCAT, HE
CCC, HF
CCCA, HG
CCCC, HH
CCCG, HI
CCCT, HJ
CCG, HK
CCGA, HL
CCGC, HM
CCGG, HN
CCGT, HO
CCT, HP
CCTA, HQ
CCTC, HR
CCTG, HS
CCTT, HT
CGA, IA
CGAA, IB
CGAC, IC
CGAG, ID
CGAT, IE
CGC, IF
CGCA, IG
CGCC, IH
CGCG, II
CGCT, IJ
CGG, IK
CGGA, IL
CGGC, IM
CGGG, IN
CGGT, IO
CGT, IP
CGTA, IQ
CGTC, IR
CGTG, IS
CGTT, IT
CTA, JA
CTAA, JB
CTAC, JC
CTAG, JD
CTAT, JE
CTC, JF
CTCA, JG
CTCC, JH
CTCG, JI
CTCT, JJ
CTG, JK
CTGA, JL
CTGC, JM
CTGG, JN
CTGT, JO
CTT, JP
CTTA, JQ
CTTC, JR
CTTG, JS
CTTT, JT
GAA, LA
GAAA, LB
GAAC, LC
GAAG, LD
GAAT, LE
GAC, LF
GACA, LG
GACC, LH
GACG, LI
GACT, LJ
GAG, LK
GAGA, LL
GAGC, LM
GAGG, LN
GAGT, LO
GAT, LP
GATA, LQ
GATC, LR
GATG, LS
GATT, LT
GCA, MA
GCAA, MB
GCAC, MC
GCAG, MD
GCAT, ME
GCC, MF
GCCA, MG
GCCC, MH
GCCG, MI
GCCT, MJ
GCG, MK
GCGA, ML
GCGC, MM
GCGG, MN
GCGT, MO
GCT, MP
GCTA, MQ
GCTC, MR
GCTG, MS
GCTT, MT
GGA, NA
GGAA, NB
GGAC, NC
GGAG, ND
GGAT, NE
GGC, NF
GGCA, NG
GGCC, NH
GGCG, NI
GGCT, NJ
GGG, NK
GGGA, NL
GGGC, NM
GGGG, NN
GGGT, NO
GGT, NP
GGTA, NQ
GGTC, NR
GGTG, NS
GGTT, NT
GTA, OA
GTAA, OB
GTAC, OC
GTAG, OD
GTAT, OE
GTC, OF
GTCA, OG
GTCC, OH
GTCG, OI
GTCT, OJ
GTG, OK
GTGA, OL
GTGC, OM
GTGG, ON
GTGT, OO
GTT, OP
GTTA, OQ
GTTC, OR
GTTG, OS
GTTT, OT
TAA, QA
TAAA, QB
TAAC, QC
TAAG, QD
TAAT, QE
TAC, QF
TACA, QG
TACC, QH
TACG, QI
TACT, QJ
TAG, QK
TAGA, QL
TAGC, QM
TAGG, QN
TAGT, QO
TAT, QP
TATA, QQ
TATC, QR
TATG, QS
TATT, QT
TCA, RA
TCAA, RB
TCAC, RC
TCAG, RD
TCAT, RE
TCC, RF
TCCA, RG
TCCC, RH
TCCG, RI
TCCT, RJ
TCG, RK
TCGA, RL
TCGC, RM
TCGG, RN
TCGT, RO
TCT, RP
TCTA, RQ
TCTC, RR
TCTG, RS
TCTT, RT
TGA, SA
TGAA, SB
TGAC, SC
TGAG, SD
TGAT, SE
TGC, SF
TGCA, SG
TGCC, SH
TGCG, SI
TGCT, SJ
TGG, SK
TGGA, SL
TGGC, SM
TGGG, SN
TGGT, SO
TGT, SP
TGTA, SQ
TGTC, SR
TGTG, SS
TGTT, ST
TTA, TA
TTAA, TB
TTAC, TC
TTAG, TD
TTAT, TE
TTC, TF
TTCA, TG
TTCC, TH
TTCG, TI
TTCT, TJ
TTG, TK
TTGA, TL
TTGC, TM
TTGG, TN
TTGT, TO
TTT, TP
TTTA, TQ
TTTC, TR
TTTG, TS
TTTT, TT
 */
