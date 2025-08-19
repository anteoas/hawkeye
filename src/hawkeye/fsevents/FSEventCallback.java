package hawkeye.fsevents;

import com.sun.jna.Callback;
import com.sun.jna.Pointer;

public interface FSEventCallback extends Callback {
    void callback(Pointer streamRef, 
                  Pointer clientCallBackInfo, 
                  long numEvents, 
                  Pointer eventPaths, 
                  Pointer eventFlags, 
                  Pointer eventIds);
}