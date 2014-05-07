/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package resourcemanager.system.peer.rm;

import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;

/**
 *
 * @author Linus
 */
public class TaskFinished extends Timeout {
    private int numCpus, amountMemInMb;
    
    public TaskFinished(ScheduleTimeout st, int numCpus, int amountMemInMb) {
        super(st);
        this.numCpus = numCpus;
        this.amountMemInMb = amountMemInMb;
    }
    public int getNumCpus() {
        return numCpus;
    }

    public int getAmountMemInMb() {
        return amountMemInMb;
    }
}
