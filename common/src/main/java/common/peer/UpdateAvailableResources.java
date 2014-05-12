/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package common.peer;

import se.sics.kompics.Event;

/**
 *
 * @author Linus
 */
public class UpdateAvailableResources extends Event {
    private AvailableResources availableResources;
    
    public UpdateAvailableResources(AvailableResources availableResources) {
        this.availableResources = availableResources;
    }

    public AvailableResources getAvailableResources() {
        return availableResources;
    }
    
}
