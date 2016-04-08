#ifndef _RANDOM_SELECTOR_
#define _RANDOM_SELECTOR_

namespace slipstore {
	class random_selector {
	      unsigned long *permutation;
	      unsigned long machines;
	      unsigned long active_machines;
	      struct drand48_data rand_buffer;
	  public:
	      random_selector(unsigned long machines_in, unsigned long me)
	          : machines(machines_in), active_machines(machines_in) {
	        srand48_r(me, &rand_buffer);
	        permutation = new unsigned long[machines];
	        for (unsigned long i = 0; i < machines; i++) {
	          permutation[i] = i;
	        }
	      }

	      unsigned long select() {
	        if (active_machines == 0) {
	          return ULONG_MAX;
	        }
	        double r;
	        drand48_r(&rand_buffer, &r);
	        unsigned long index = (unsigned long) (r * active_machines);
	        unsigned long mc = permutation[index];
	        permutation[index] = permutation[--active_machines];
	        return mc;
	      }

	      bool have_options() {
	        return (active_machines != 0);
	      }

	      void deselect(unsigned long mc) {
	        permutation[active_machines++] = mc;
	      }

	      void deselect_all() {
	        for (unsigned long i = 0; i < machines; i++) {
	          permutation[i] = i;
	        }
	        active_machines = machines;
	      }

	      ~random_selector() {
	        delete [] permutation;
	      }
	  };
}
#endif