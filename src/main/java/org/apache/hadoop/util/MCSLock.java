/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 * @author ashwinkayyoor
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

public class MCSLock 
{
    /* Shared tail of MCS Queue  */
    AtomicReference<QNode> tail;
    
    /* reference to this thread's private QNode */
    ThreadLocal<QNode> _localNode;
    
    /*
     * A node in the shared MCS Queue.
     * 
     * I use a !hack! here to prevent false sharing. 
     * The QNode needs to hold the boolean lock state AND a reference
     * to the QNode that follows it in the queue :
     * 
     *   boolean isLocked = false;
     *   QueueNode behind = null;
     * 
     * To prevent false sharing due to cache lines containing closely
     * packed QNode references, I make an of type QNode but only use the first
     * two elements.
     * 
     * The first element represents the lock state:
     * 
     *      A null value represents 'unlocked'
     *      A non-null value represents 'locked'
     * 
     * The second is the QNode that follows it.
     * 
     */
    class QNode
    {
        // Indexes into the padded QNode
        private static final int IS_LOCKED = 0;
        private static final int BEHIND = 1;
        
        private QNode[] paddedQNode;
                        
        /*
         * Initializes a QNode with a null 'behind' reference
         * and is initially unlocked.
         */
        public QNode()
        {
            // This could be as much as 8 times bigger than a standard
            // 128 byte cache line but maintains cross-platform portability.
            // @TODO Potential to reduce memory usage here!
            //cache line size for tibanna is 64 bytes
            paddedQNode = new QNode[64];            
            paddedQNode[IS_LOCKED] = null;
            paddedQNode[BEHIND] = null;
        }
                
        /*
         * @return returns true if the QNode is locked, false otherwise.
         */
        public boolean getIsLocked()
        {
            // null <--> unlocked ; !null <--> locked
            return paddedQNode[IS_LOCKED] != null;
        }
        
        public void setIsLocked(boolean locked)
        {
            // null <--> unlocked ; !null <--> locked
            paddedQNode[IS_LOCKED] = locked ? this : null;
        }
        
        public QNode getBehind() {
            return paddedQNode[BEHIND];
        }        
        
        public void setBehind(QNode qNode) {
            paddedQNode[BEHIND] = qNode;
        }
    }  
    
    public MCSLock()
    {   
        tail = new AtomicReference(null);    
        _localNode = new ThreadLocal<QNode>() {                  
            @Override
            protected QNode initialValue()
            {
                return new QNode();
            }      
        };    
    }
    
    public void acquire()
    {
        QNode qNode = _localNode.get();        
        
        // attempt to atomically read current tail and replace it with ourselves
        QNode ahead = tail.getAndSet(qNode);
        
        // someone in queue ahead of us...
        if(ahead != null)
        {
            // Mutex currently locked
            qNode.setIsLocked(true);
            
            // point guy ahead of us to us
            ahead.setBehind(qNode);
            
            // spin until guy ahead of us acquires and releases the lock (at which
            // point the guy ahead of will also set this.qNode.setIsLocked(false) )
            while(qNode.getIsLocked()) {}
        }
        
        // else
        //      we can fast-path it to the CS
    }
    
    public void release()
    {
        QNode qNode = _localNode.get();
        
        // anyone added themsleves to queue behind us while we were in CS?
        
        // case A : No ; case B : yes, but haven't updated our .behind ref yet
        if(qNode.getBehind() == null)
        {
            // IF we are only guy in queue, the CAS should succeed
            if(tail.compareAndSet(qNode, null))
                return;
            
            // we now KNOW we have a guy behind us, but we don't know who.
            // wait for him to update our behind reference
            while(qNode.getBehind() == null) {}
        }
        
        // now we can pass the lock to him
        qNode.getBehind().setIsLocked(false);
        qNode.setBehind(null);
    }
}