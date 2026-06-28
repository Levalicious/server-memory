/* Validate the biscuit-derived versioned record machinery on our Entity schema. */
#include <stdio.h>
#include <assert.h>
#include "entity.h"

int main(void) {
    int ok = 1;
    printf("sizeof(Entity)=%zu (expect 72)\n", sizeof(Entity));
    printf("entity_bufsize(1)=%zu (expect 76)\n", entity_bufsize(1));
    if (sizeof(Entity) != 72) ok = 0;
    if (entity_bufsize(1) != 76) ok = 0;

    Entity e; memset(&e, 0, sizeof(e));
    e.name_id = 42; e.type_id = 7; e.adj_offset = 0x1234; e.mtime = 999;
    e.obs_count = 2; e.obs0_id = 11; e.obs1_id = 12;
    e.structural_visits = 5; e.walker_visits = 9; e.psi = 3.14159;

    u8 buf[128];
    entity_write(&e, buf);
    u32 stored_ver = *(u32 *)buf;

    Entity r;
    int rc = entity_read(buf, &r);
    printf("read rc=%d stored_ver=%u name_id=%u psi=%g walker=%llu obs1=%u\n",
           rc, stored_ver, r.name_id, r.psi, (unsigned long long)r.walker_visits, r.obs1_id);
    if (rc != 0 || stored_ver != 1) ok = 0;
    if (r.name_id != 42 || r.type_id != 7 || r.adj_offset != 0x1234 || r.mtime != 999) ok = 0;
    if (r.obs_count != 2 || r.obs0_id != 11 || r.obs1_id != 12) ok = 0;
    if (r.structural_visits != 5 || r.walker_visits != 9 || r.psi != 3.14159) ok = 0;

    printf(ok ? "\nALL PASS\n" : "\nFAILED\n");
    return ok ? 0 : 1;
}
