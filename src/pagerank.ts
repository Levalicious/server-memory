/**
 * PageRank sampling — structural rank via random walks on graph topology.
 *
 * Implements MC complete-path stopping at dangling nodes (Algorithm 4 from
 * Avrachenkov et al. "Monte Carlo methods in PageRank computation").
 *
 * One "iteration" = one random walk starting from each node in the graph.
 * Each walk follows forward edges with probability c (damping factor),
 * terminates with probability (1-c) at each step, and stops immediately
 * at dangling nodes (nodes with no forward edges).
 *
 * Every node visited along the walk gets its structuralVisits incremented.
 * The global structuralTotal tracks the sum across all visits.
 * PageRank(j) = structuralVisits(j) / structuralTotal.
 *
 * For 14K nodes with c=0.85, one iteration produces ~93K visits total
 * (14K starts × ~6.67 avg walk length) and gives <7% error for top pages.
 */

import { type GraphFile, DIR_FORWARD } from './graphfile.js';

const DEFAULT_DAMPING = 0.85;

/**
 * Run one full iteration of structural PageRank sampling.
 * Starts one random walk from every node in the graph.
 *
 * @param gf      GraphFile to sample on
 * @param damping Probability of following a link (vs terminating). Default 0.85.
 * @returns       Total number of visits recorded in this iteration.
 */
export function structuralIteration(gf: GraphFile, damping: number = DEFAULT_DAMPING): number {
  const offsets = gf.getAllEntityOffsets();
  let totalVisits = 0;

  for (const startOffset of offsets) {
    totalVisits += structuralWalk(gf, startOffset, damping);
  }

  return totalVisits;
}

/**
 * Run a single structural random walk starting from `startOffset`.
 * Follows forward edges, counting every visited node (complete path).
 * Stops at dangling nodes or with probability (1-damping) at each step.
 *
 * @returns Number of visits recorded in this walk.
 */
export function structuralWalk(gf: GraphFile, startOffset: bigint, damping: number = DEFAULT_DAMPING): number {
  let current = startOffset;
  let visits = 0;

  while (true) {
    // Visit current node
    gf.incrementStructuralVisit(current);
    visits++;

    // Get forward edges only
    const edges = gf.getEdges(current);
    const forward = edges.filter(e => e.direction === DIR_FORWARD);

    // Dangling node — stop (Algorithm 4)
    if (forward.length === 0) break;

    // Terminate with probability (1 - damping)
    if (Math.random() >= damping) break;

    // Follow a random forward edge
    const idx = Math.floor(Math.random() * forward.length);
    current = forward[idx].targetOffset;
  }

  return visits;
}

/**
 * Run N iterations of structural sampling.
 * Each iteration = one walk from every node.
 *
 * @returns Total visits across all iterations.
 */
export function structuralSample(gf: GraphFile, iterations: number = 1, damping: number = DEFAULT_DAMPING): number {
  let total = 0;
  for (let i = 0; i < iterations; i++) {
    total += structuralIteration(gf, damping);
  }
  return total;
}
