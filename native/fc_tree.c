/*
 * ACSL/WP: the Cartesian-tree fit descent (mf_fit), modeled over a node array.
 * Proves MEMORY-SAFETY of the traversal: the cursor never leaves the node array,
 * given the flat field-validity invariant (every left/right index is in range).
 *
 *   frama-c -wp -wp-rte -wp-prover alt-ergo,z3 fc_tree.c
 *
 * NOTE ON TERMINATION: the descent terminates only if the tree is ACYCLIC.
 * That acyclicity is exactly the structural invariant whose violation produced
 * the original free-list HANG. WP cannot prove termination here without an
 * acyclicity/height measure (an inductive reachability predicate + ghost state) —
 * so this file proves safety; termination-under-acyclicity is the next frontier.
 */
#include <stdint.h>

#define NCAP 1048576u

typedef struct { uint64_t size; uint32_t left, right, parent; } node;

/*@ predicate fields_in_range(node *nodes, integer n) =
  @   \forall integer k; 0 <= k < n ==>
  @       0 <= nodes[k].left < n && 0 <= nodes[k].right < n;
  @*/

/*@ requires 0 < n <= NCAP;
  @ requires 0 <= root < n;
  @ requires \valid_read(nodes + (0 .. n - 1));
  @ requires fields_in_range(nodes, n);
  @ assigns \nothing;
  @ ensures 0 <= \result < n;
  @*/
uint32_t mf_fit_model(node *nodes, uint32_t n, uint32_t root, uint64_t need) {
    uint32_t cur = root;
    /*@ loop invariant 0 <= cur < n;
      @ loop assigns cur;
      @*/
    while (cur != 0) {
        uint32_t l = nodes[cur].left;
        if (l != 0 && nodes[l].size >= need) { cur = l; continue; }
        if (nodes[cur].size >= need) return cur;
        cur = nodes[cur].right;
    }
    return 0;
}
