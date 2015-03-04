package Brush;

import org.trifort.rootbeer.runtime.Kernel;

/**
 * Created by Mike Busch on 2/25/15.
 */
public class StatsMapKernel implements Kernel
{
    long smallcnt = 0;
    long smallsum = 0;
    long smalldeg = 0;
    double smallcov = 0;
    long medcnt = 0;
    long medsum = 0;
    long meddeg = 0;
    double medcov = 0;
    int len = 0;
    int fdegree = 0;
    int rdegree = 0;
    float cov = 0;
    int ONE = 0;

    public StatsMapKernel(int in_len, int in_fdegree, int in_rdegree, int in_cov)
    {
        fdegree = in_fdegree;
        rdegree = in_rdegree;
        cov = in_cov;
        len = in_len;
    }

    @Override
    public void gpuMethod()
    {
        if (len >= 50)
        {
            medcnt++;
            medsum += len;
            meddeg += (fdegree + rdegree) * len;
            medcov += cov * len;
            ONE++;
        }

        smallcnt++;
        smallsum += len;
        smalldeg += (fdegree + rdegree) * len;
        smallcov += cov * len;
    }
}
