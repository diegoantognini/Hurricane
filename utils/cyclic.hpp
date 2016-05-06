#ifndef _CYCLIC_
#define _CYCLIC_

namespace hurricane {
  class cyclic {
      unsigned long *permutation;
      unsigned long cyclic_index;
      unsigned long machines;
      struct drand48_data rand_buffer;
  public:
      cyclic(unsigned long machines_in, unsigned long me)
          : machines(machines_in) {
        srand48_r(me, &rand_buffer);
        permutation = new unsigned long[machines];
        permutation[0] = 0;
        for (unsigned long i = 1; i < machines; i++) {
          permutation[i] = i;
          double r;
          drand48_r(&rand_buffer, &r);
          unsigned long interchange = (unsigned long) (r * (i + 1));
          unsigned long tmp = permutation[interchange];
          permutation[interchange] = permutation[i];
          permutation[i] = tmp;
        }
        cyclic_index = 0;
      }

      unsigned long cyclic_next() {
        unsigned long mc = permutation[cyclic_index];
        cyclic_index = (cyclic_index + 1) % machines;
        return mc;
      }

      unsigned long cycle_size() {
        return machines;
      }

      ~cyclic() {
        delete permutation;
      }
  };
}

#endif