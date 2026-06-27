// Smoke test for the v3 graphstore N-API addon (C<->JS marshaling).
const native = require('../../build/Release/graphstore.node');
const fs = require('fs');
const j = (v) => JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString() : x));

for (const f of ['/tmp/gb_g.dat', '/tmp/gb_s.dat']) { try { fs.unlinkSync(f); } catch {} }
const h = native.open('/tmp/gb_g.dat', '/tmp/gb_s.dat', 65536);

const alice = native.createEntity(h, 'Alice', 'Person', 100n);
const bob   = native.createEntity(h, 'Bob', 'Person', 100n);
const acme  = native.createEntity(h, 'Acme', 'Org', 100n);
native.createRelation(h, alice, bob, 'KNOWS', 100n);
native.createRelation(h, alice, acme, 'WORKS_AT', 100n);
native.addObservation(h, alice, 'likes tea', 100n);

let ok = true;
const expect = (cond, msg) => { if (!cond) { ok = false; console.log('FAIL:', msg); } else console.log('ok:  ', msg); };

expect(native.entityCount(h) === 3, 'entityCount == 3');
expect(native.relationCount(h) === 2, 'relationCount == 2');
expect(native.lookup(h, 'Alice') === alice, 'lookup Alice round-trips');
expect(native.lookup(h, 'Nobody') === 0n, 'lookup miss -> 0n');

const arec = native.readEntity(h, alice);
expect(arec.name === 'Alice' && arec.type === 'Person', 'readEntity name/type');
expect(arec.observations.length === 1 && arec.observations[0] === 'likes tea', 'readEntity observations');

const nbrs = native.neighbors(h, alice, 1, 0).map((o) => native.entityName(h, o)).sort();
expect(j(nbrs) === j(['Acme', 'Bob']), 'neighbors(Alice, fwd, d1) = [Acme, Bob]');

const path = native.findPath(h, bob, acme, 4, 255).map((o) => native.entityName(h, o));
expect(j(path) === j(['Bob', 'Alice', 'Acme']), 'findPath Bob->Acme (any) = Bob,Alice,Acme');

expect(native.search(h, 'Person').length === 2, 'search Person -> 2');
expect(native.entityTypes(h).sort().join(',') === 'Org,Person', 'entityTypes');
expect(native.relationTypes(h).sort().join(',') === 'KNOWS,WORKS_AT', 'relationTypes');

native.structuralSample(h, 1, 0.85);
native.computeMerwPsi(h, 0.85, 200, 1e-8);
expect(native.structuralTotal(h) > 0n, 'structural sampling recorded visits');
expect(native.getPsi(h, alice) > 0, 'merw psi populated');
native.incWalkerVisit(h, alice);
expect(native.walkerTotal(h) === 1n && native.walkerRank(h, alice) > 0, 'walker counting works');

native.sync(h);
native.close(h);
console.log(ok ? '\nALL PASS' : '\nFAILED');
process.exit(ok ? 0 : 1);   // exits clean: binding.gyp drops -lnode, so no 2nd libnode/libstdc++ static-dtor teardown
