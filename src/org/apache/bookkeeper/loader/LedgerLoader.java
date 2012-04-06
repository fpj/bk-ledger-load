package org.apache.bookkeeper.loader;

import java.util.ArrayList;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;

public class LedgerLoader {

    static public void main (String args[]) {
        try{
            ArrayList<Long> list = new ArrayList<Long>();
            int threshold = Integer.parseInt(args[0]);
            String zkserver = args[1];
            BookKeeper bk = new BookKeeper(zkserver);
        
            while(list.size() < threshold){
                LedgerHandle lh = bk.createLedger(3, 2, DigestType.CRC32, "bk is cool".getBytes());
                System.out.println("Ledger id: " + lh.getId());
                
                for(int i = 0; i < 5 ; i++){
                    lh.addEntry(new byte[1024]);
                }
                list.add(lh.getId());
                lh.close();
            }
            
            while(true){
                bk.deleteLedger(list.get(0));
                System.out.println("Deleted ledger id: " + list.get(0));
                list.remove(0);
                
                LedgerHandle lh = bk.createLedger(3, 2, DigestType.CRC32, "bk is cool".getBytes());
                System.out.println("Created ledger id: " + lh.getId());
                
                for(int i = 0; i < 5 ; i++){
                    lh.addEntry(new byte[1024]);
                }
                
                list.add(lh.getId());
            }
        
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    
}
