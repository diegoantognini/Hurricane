#ifndef _BUSY_COUNTER_
#define _BUSY_COUNTER_

  struct busy_counter {
      rtc_clock counter;
      unsigned long users;
      volatile unsigned long lock;

      busy_counter()
          : users(0),
            lock(0) { }

      void start() {
        while (!__sync_bool_compare_and_swap(&lock, 0, 1));
        users++;
        if (users == 1) {
          counter.start();
        }
        __sync_synchronize();
        __sync_bool_compare_and_swap(&lock, 1, 0);
      }

      void stop() {
        while (!__sync_bool_compare_and_swap(&lock, 0, 1));
        users--;
        if (users == 0) {
          counter.stop();
        }
        __sync_synchronize();
        __sync_bool_compare_and_swap(&lock, 1, 0);
      }
  };

#endif