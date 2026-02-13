/**
 * Maximum Entropy Random Walk (MERW) — dominant eigenvector computation
 * via power iteration on the graph's adjacency matrix.
 *
 * MERW transition probabilities:  S_ij = (A_ij / λ) * (ψ_j / ψ_i)
 * Stationary distribution:        ρ_i  = ψ_i² / ‖ψ‖₂²
 *
 * We compute ψ (the dominant right eigenvector of A) using sparse power
 * iteration directly on the GraphFile adjacency lists. No dense matrix
 * is ever constructed.
 *
 * For directed graphs that may not be strongly connected, we add
 * teleportation damping (like PageRank): at each step, follow an edge
 * with probability `alpha`, or jump to a uniform random node with
 * probability `(1 - alpha)`. This guarantees convergence to a unique
 * positive eigenvector.
 */

import { type GraphFile, DIR_FORWARD } from './graphfile.js';

const DEFAULT_ALPHA = 0.85;
const DEFAULT_MAX_ITER = 200;
const DEFAULT_TOL = 1e-8;

/**
 * Compute the dominant eigenvector of the (damped) adjacency matrix
 * via power iteration and write ψ_i into each entity record.
 *
 * Warm-starts from the ψ values already stored in the entity records.
 * New nodes (psi === 0) are seeded with the mean of existing values.
 * On a fresh graph (all zeros), falls back to uniform initialization.
 *
 * @param gf      GraphFile to operate on
 * @param alpha   Damping factor (probability of following an edge). Default 0.85.
 * @param maxIter Maximum iterations. Default 200.
 * @param tol     Convergence tolerance (L2 norm of change). Default 1e-8.
 * @returns       Number of iterations performed.
 */
export function computeMerwPsi(
  gf: GraphFile,
  alpha: number = DEFAULT_ALPHA,
  maxIter: number = DEFAULT_MAX_ITER,
  tol: number = DEFAULT_TOL,
): number {
  const offsets = gf.getAllEntityOffsets();
  const n = offsets.length;
  if (n === 0) return 0;

  // Build offset → index map for O(1) lookup
  const indexMap = new Map<bigint, number>();
  for (let i = 0; i < n; i++) {
    indexMap.set(offsets[i], i);
  }

  // Build sparse adjacency: for each node, list of forward neighbor indices
  const adj: number[][] = new Array(n);
  for (let i = 0; i < n; i++) {
    const edges = gf.getEdges(offsets[i]);
    const neighbors: number[] = [];
    for (const e of edges) {
      if (e.direction !== DIR_FORWARD) continue;
      const j = indexMap.get(e.targetOffset);
      if (j !== undefined) neighbors.push(j);
    }
    adj[i] = neighbors;
  }

  // Warm-start: read existing ψ from entity records
  let psi = new Float64Array(n);
  let hasWarm = false;
  let warmSum = 0;
  let warmCount = 0;

  for (let i = 0; i < n; i++) {
    const val = gf.getPsi(offsets[i]);
    psi[i] = val;
    if (val > 0) {
      hasWarm = true;
      warmSum += val;
      warmCount++;
    }
  }

  if (hasWarm) {
    // Seed new/zero nodes with the mean of existing nonzero values
    const mean = warmSum / warmCount;
    for (let i = 0; i < n; i++) {
      if (psi[i] <= 0) psi[i] = mean;
    }
  } else {
    // Cold start: uniform
    const uniform = 1.0 / Math.sqrt(n);
    psi.fill(uniform);
  }

  // Normalize initial vector to unit L2
  let initNorm = 0;
  for (let i = 0; i < n; i++) initNorm += psi[i] * psi[i];
  initNorm = Math.sqrt(initNorm);
  if (initNorm > 0) {
    for (let i = 0; i < n; i++) psi[i] /= initNorm;
  }

  let psiNext = new Float64Array(n);

  const teleport = (1.0 - alpha) / n;

  let iter = 0;
  for (iter = 0; iter < maxIter; iter++) {
    // Matrix-vector multiply: psiNext = alpha * A * psi + (1-alpha)/n * sum(psi)
    // Since ψ is normalized, sum(psi) components contribute uniformly.
    // For the adjacency multiply, A_ij = 1 if edge i→j exists.
    // Power iteration: psiNext_j = alpha * Σ_{i: i→j} psi_i  +  teleport * Σ_k psi_k
    //
    // We iterate over source nodes and scatter to targets.

    psiNext.fill(0);

    // Compute sum of psi for teleportation
    let psiSum = 0;
    for (let i = 0; i < n; i++) psiSum += psi[i];

    const teleportContrib = teleport * psiSum;

    // Sparse multiply: scatter from sources to targets
    for (let i = 0; i < n; i++) {
      const neighbors = adj[i];
      const val = alpha * psi[i];
      for (const j of neighbors) {
        psiNext[j] += val;
      }
    }

    // Add teleportation
    for (let i = 0; i < n; i++) {
      psiNext[i] += teleportContrib;
    }

    // Normalize to unit L2
    let norm = 0;
    for (let i = 0; i < n; i++) norm += psiNext[i] * psiNext[i];
    norm = Math.sqrt(norm);
    if (norm > 0) {
      for (let i = 0; i < n; i++) psiNext[i] /= norm;
    }

    // Check convergence: L2 norm of difference
    let diff = 0;
    for (let i = 0; i < n; i++) {
      const d = psiNext[i] - psi[i];
      diff += d * d;
    }
    diff = Math.sqrt(diff);

    // Swap buffers
    const tmp = psi;
    psi = psiNext;
    psiNext = tmp;

    if (diff < tol) {
      iter++;
      break;
    }
  }

  // Ensure all components are positive (Perron-Frobenius: dominant eigenvector is non-negative,
  // but numerical noise can produce tiny negatives). Clamp to 0.
  for (let i = 0; i < n; i++) {
    if (psi[i] < 0) psi[i] = 0;
  }

  // Write ψ_i into each entity record
  for (let i = 0; i < n; i++) {
    gf.setPsi(offsets[i], psi[i]);
  }

  return iter;
}
