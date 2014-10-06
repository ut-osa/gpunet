GPUnet
======

GPUnet is a native GPU networking layer that provides a reliable stream abstraction over Infiniband  and high-level socket APIs  to GPU programs for NVIDIA GPUs.

GPUnet enables threads or threadblocks in one GPU to communicate with threads in other GPUs or CPUs via standard and familiar socket interfaces, regardless of whether they are in the same or different machines. 

GPUnet uses Peer-to-Peer DMA (via GPUDirectRDMA) to place and manage network buffers of a GPU application directly in  GPU memory.  

Code example
============
This is a code example of a simple (working) GPU echo client. 

Note that  the GPU socket API is threadblock-cooperative, meaning that all the threads in the threadblock are required to call the same function with the same parameters at the same point in a program.

    __global__ void gpuclient(struct sockaddr_in *addr, int* tb_alloc_tbl, int nr_tb) {
      __shared__ int sock;
      __shared__ uchar buf[BUF_SIZE];
      int ret, i;
    
      while ((sock = gconnect_in(addr)) < 0) {};
      assert(sock >= 0);

      for (i = 0; i < NR_MSG; i++) {
        int recved = 0, sent = 0;
    
        do {
          ret = gsend(sock, buf + sent, BUF_SIZE - sent);
          if (ret < 0) {
            goto out;
          } else {
            sent += ret;
          }
        } while (sent < BUF_SIZE);
        
        __syncthreads(); 
      
        do {
          ret = grecv(sock, buf + recved, BUF_SIZE - recved);
          if (ret < 0) {
            goto out;
          } else {
            recved += ret;
          }
        } while (recved < BUF_SIZE);
        
        __syncthreads();
      }

      out:
      BEGIN_SINGLE_THREAD_PART {
          single_thread_gclose(sock);
      } END_SINGLE_THREAD_PART;
    }

