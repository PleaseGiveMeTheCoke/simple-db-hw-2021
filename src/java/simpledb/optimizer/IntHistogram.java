package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;

    private int min;

    private int max;

    private int[] histogram;


    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = buckets;
        this.max = max;
        this.min = min;
        histogram = new int[buckets];

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        try {

            double unit = (double) (max - min + 1) / buckets;


            int i = (int) Math.floor((v - min) / unit);


            histogram[i]++;
        }catch (Exception e){
            System.out.println(v);
        }

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int ntups = 0;
        for (int num : histogram) {
            ntups+=num;
        }

        if(op.equals(Predicate.Op.EQUALS)){
            if(v<min||v>max){
                return 0.0;
            }

            double w =(double) (max - min + 1) / buckets;

            int i = (int) Math.floor((v - min) / w);

            int h = histogram[i];
            w = Math.max(1,w);
            return (double) (h / w) / ntups;

           // return 1-estimateSelectivity(Predicate.Op.GREATER_THAN, v)-estimateSelectivity(Predicate.Op.LESS_THAN,v);

        }else if(op.equals(Predicate.Op.NOT_EQUALS)){
            if(v<min||v>max){
                return 1.0;
            }

            return 1-estimateSelectivity(Predicate.Op.EQUALS, v);
        }else if(op.equals(Predicate.Op.GREATER_THAN)){
            if(v<min){
                return 1.0;
            }

            if(v>max){
                return 0.0;
            }
            double w_b= (double) (max - min + 1) / buckets;


            int i = (int) Math.floor((v - min) / w_b);

            int h_b = histogram[i];

            double b_f = (double) h_b / ntups;

            int b_right = (int) (min+(i+1)*w_b-1);
            w_b = Math.max(1,w_b);
            double b_part = (double) (b_right - v) / w_b;

            double sum = b_f*b_part;
            for (int j = i+1; j < buckets; j++) {
                sum+=(double) histogram[j]/ntups;
            }
            return sum;

        }else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
            if(v<min){
                return 0.0;
            }

            if(v>=max){
                return 1.0;
            }

            return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
        }else if(op.equals(Predicate.Op.LESS_THAN)){
            if(v<min){
                return 0.0;
            }

            if(v>max){
                return 1.0;
            }

            double w_b= (double) (max - min + 1) / buckets;


            int i = (int) Math.floor((v - min) / w_b);

            int h_b = histogram[i];

            double b_f = (double) h_b / ntups;

            int b_left = (int)(min+i*w_b);

            w_b = Math.max(1,w_b);
            double b_part = (double) (v-b_left) / w_b;

            double sum = b_f*b_part;
            for (int j = i-1; j >= 0; j--) {
                sum+=(double) histogram[j]/ntups;
            }
            return sum;
        }else if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
            if(v<=min){
                return 1.0;
            }

            if(v>max){
                return 0.0;
            }

            return 1-estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }else{

            System.out.println(op.toString()+"don't support");
            return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "min = "+this.min+"\n"
                +"max = "+this.max+"\n"
                + Arrays.toString(histogram)+"\n";
    }
}
