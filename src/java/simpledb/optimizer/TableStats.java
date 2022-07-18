package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private int pageNum;

    private int ioCostPerPage;

    private DbFile databaseFile;

    private int tuplesNum;

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private ArrayList<Integer> intIndexes;
    private ArrayList<Integer> stringIndexes;
    private int[] maxValues;
    private int[] minValues;
    private IntHistogram[] intHistograms;
    private StringHistogram[] stringHistograms;
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        this.databaseFile = databaseFile;
        this.intIndexes = new ArrayList<>();
        this.stringIndexes = new ArrayList<>();
        TupleDesc tupleDesc = databaseFile.getTupleDesc();
        int numberFields = tupleDesc.numFields();
        for (int i = 0; i < numberFields; i++) {
            if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)){
                intIndexes.add(i);
            }else{
                stringIndexes.add(i);
            }
        }
        maxValues = new int[intIndexes.size()];
        Arrays.fill(maxValues,Integer.MIN_VALUE);
        minValues = new int[intIndexes.size()];
        Arrays.fill(minValues,Integer.MAX_VALUE);
        intHistograms = new IntHistogram[intIndexes.size()];
        stringHistograms = new StringHistogram[stringIndexes.size()];


        DbFileIterator iterator = databaseFile.iterator(new TransactionId());
        try {
            iterator.open();
            while (iterator.hasNext()){
                tuplesNum++;
                Tuple next = iterator.next();
                this.pageNum = next.getRecordId().getPageId().getPageNumber();
                for (int i = 0; i < intIndexes.size(); i++) {
                    int index = intIndexes.get(i);
                    Field field = next.getField(index);
                    minValues[i] = Math.min(minValues[i],Integer.parseInt(field.toString()));
                    maxValues[i] = Math.max(maxValues[i],Integer.parseInt(field.toString()));
                }
            }
            for (int i = 0; i < intIndexes.size(); i++) {
                intHistograms[i] = new IntHistogram(NUM_HIST_BINS,minValues[i],maxValues[i]);
            }
            for (int i = 0; i < stringIndexes.size(); i++) {
                stringHistograms[i] = new StringHistogram(NUM_HIST_BINS);
            }

            iterator.rewind();
            while (iterator.hasNext()){
                Tuple next = iterator.next();
                for (int i = 0; i < intIndexes.size(); i++) {
                    int index = intIndexes.get(i);
                    Field field = next.getField(index);
                    int v = Integer.parseInt(field.toString());
                    intHistograms[i].addValue(v);
                }

                for (int i = 0; i < stringIndexes.size(); i++) {
                    int index = stringIndexes.get(i);
                    Field field = next.getField(index);
                    String v = field.toString();
                    stringHistograms[i].addValue(v);
                }
            }

            iterator.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return (pageNum+1)*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (tuplesNum*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(constant.getType().equals(Type.STRING_TYPE)){
            int i = -1;
            for (int j = 0; j < stringIndexes.size(); j++) {
                if(stringIndexes.get(j) == field){
                    i = j;
                    break;
                }
            }
            StringHistogram histogram = stringHistograms[i];
            return histogram.estimateSelectivity(op,constant.toString());
        }else{
            int i = -1;
            for (int j = 0; j < intIndexes.size(); j++) {
                if(intIndexes.get(j) == field){
                    i = j;
                    break;
                }
            }
            IntHistogram histogram = intHistograms[i];
            return histogram.estimateSelectivity(op,Integer.parseInt(constant.toString()));
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tuplesNum;
    }

}
