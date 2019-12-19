Brain dump for shared process namespace and data sidecar

 - Downloading data in init works fine :). Downloader is easy
 - Uploader is where all the complications are,
   
   Goal:
     - We want to 1. identify the main container
                  2. Wait for the main container to start up
                  3. Wait for the main container to exit
                  4. Copy all the data over
                  5. Exit ourselves

  Solution 1:
     poll Kubeapi.
     - Works perfectly fine, but too much load on kubeapi

  Solution 2:
    Create a protocol. Main container will exit and write a _SUCCESS file to a known location
    - problem in the case of oom or random exits. Uploader will be stuck. We could use a timeout? and in the sidecar just kill the pod, when the main exits unhealthy?

  Solution 3:
    Use shared process namespace. This allows all pids in a pod to share the namespace. Thus pids can see each other.

    Problems:
     How to identify the main container?
       - Container id is not known ahead of time and container name -> Pid mapping is not possible?
       - How to wait for main container to start up.
          One solution for both, call kubeapi and get pod info and find the container id
       
    Note: we can poll /proc/pid/cgroup file (it contains the container id) so we can create a blind container id to pid mapping. Then somehow get the main container id

    Once we know the main container, waiting for it to exit is simple and implemented
    Copying data is simple and implemented
     
