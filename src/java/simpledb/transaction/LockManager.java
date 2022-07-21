package simpledb.transaction;

import simpledb.storage.Page;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    volatile ArrayList<TransactionId> readLock;

    volatile TransactionId writeLock;

    public static Map<PageId,LockManager> plMap = new HashMap<>();

    public static Map<TransactionId, Set<PageId>> tpMap = new HashMap<>();

    public static Map<TransactionId,TNode> nodeMap = new HashMap<>();

    public LockManager(){
        readLock = new ArrayList<>();
        writeLock = null;
    }
    public static void reset(){
        plMap = new HashMap<>();
        tpMap = new HashMap<>();
    }
    private static void addTpMap(PageId pid,TransactionId tid){
        if(tpMap.get(tid) == null){
            tpMap.put(tid,new HashSet<>());
            if(nodeMap.get(tid) == null) {
                nodeMap.put(tid, new TNode(tid));
            }
        }

        Set<PageId> pageIds = tpMap.get(tid);
        pageIds.add(pid);
    }
    public synchronized static boolean getReadLock(PageId pid,TransactionId tid) throws TransactionAbortedException {
        if(plMap.get(pid) == null){
            LockManager lm = new LockManager();
            lm.readLock.add(tid);
            plMap.put(pid,lm);
            addTpMap(pid,tid);
            return true;
        }else{
            LockManager lockManager = plMap.get(pid);
            if(lockManager.writeLock != null){
                if(lockManager.writeLock != tid){
                    //建立依赖

                    nodeMap.getOrDefault(tid,new TNode(tid)).next = nodeMap.get(lockManager.writeLock);
                    //死锁检测
                    deadLockDetect(tid,lockManager.writeLock);
                    return false;
                }
                //消除依赖
                nodeMap.get(tid).next = null;
                return true;
            }else{
                lockManager.readLock.add(tid);
                addTpMap(pid,tid);
                //消除依赖
                nodeMap.get(tid).next = null;
                return true;
            }
        }
    }

    public synchronized static boolean getWriteLock(PageId pid,TransactionId tid) throws TransactionAbortedException {
        if(plMap.get(pid) == null){
            LockManager lm = new LockManager();
            lm.writeLock = tid;
            plMap.put(pid,lm);
            addTpMap(pid,tid);
            return true;
        }else{
            LockManager lockManager = plMap.get(pid);
            if(lockManager.writeLock != null){
                if(lockManager.writeLock != tid){
                    //建立依赖
                    nodeMap.getOrDefault(tid,new TNode(tid)).next = nodeMap.get(lockManager.writeLock);
                    //死锁检测
                    deadLockDetect(tid,lockManager.writeLock);
                    return false;
                }
                //消除依赖
                nodeMap.get(tid).next = null;
                return true;
            }else if(lockManager.readLock.size()!=0){
                if(lockManager.readLock.size() == 1 && lockManager.readLock.get(0).equals(tid)){
                    //锁升级
                    lockManager.readLock.clear();
                    lockManager.writeLock = tid;
                    //消除依赖
                    nodeMap.get(tid).next = null;
                    return true;
                }
                //建立依赖
                for (TransactionId transactionId : lockManager.readLock) {
                    if(!transactionId.equals(tid)){
                        nodeMap.getOrDefault(tid,new TNode(tid)).next = nodeMap.get(lockManager.readLock.get(0));
                        break;
                    }
                }
                //死锁检测
                deadLockDetect(tid,lockManager.readLock.get(0));
                return false;
            }else{
                lockManager.writeLock = tid;
                addTpMap(pid,tid);
                //消除依赖
                nodeMap.get(tid).next = null;
                return true;
            }
        }
    }

    public synchronized static void releaseLock(PageId pid,TransactionId tid){
        LockManager lockManager = plMap.get(pid);
        if(lockManager.writeLock == tid){
            lockManager.writeLock = null;
        }else{
            ArrayList<TransactionId> readLocks = lockManager.readLock;
            readLocks.remove(tid);
        }
        tpMap.get(tid).remove(pid);
    }

    public synchronized static boolean holdsLock(TransactionId tid, PageId p) {
        if(plMap.get(p) == null){
            return false;
        }

        LockManager lockManager = plMap.get(p);
        if(lockManager.writeLock == tid){
            return true;
        }else if(lockManager.writeLock == null){
            return lockManager.readLock.contains(tid);
        }
        return false;
    }

    public  static void releaseAllLock(TransactionId tid){
        //消除依赖
        if(nodeMap.get(tid)!=null) {
            nodeMap.get(tid).next = null;
            nodeMap.remove(tid);
        }
        Set<PageId> pageIds = tpMap.get(tid);
        if(pageIds == null){
            return;
        }
        PageId[] ar = new PageId[pageIds.size()];
        int i = 0;
        for (PageId pageId : pageIds) {
            ar[i] = pageId;
            i++;
        }
        for (PageId pageId : ar) {
            releaseLock(pageId, tid);
        }

    }

    static class TNode{
        TransactionId tid;
        TNode next;

        public TNode(TransactionId tid) {
            this.tid = tid;
        }
    }

    /**
     * tid1 等待 tid2释放锁，该过程是否形成死锁
     * @param tid1
     * @param tid2
     * @throws TransactionAbortedException
     */
    private static void deadLockDetect(TransactionId tid1,TransactionId tid2) throws TransactionAbortedException {
        TNode node = nodeMap.get(tid2);

        while (node!=null&&node.next!=null){
            if(node.next.tid.equals(tid1)){
                throw new TransactionAbortedException();
            }else{
                node = node.next;
            }
        }

    }



}
