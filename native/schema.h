// schema.h - Bidirectional lens framework for versioned structs
#ifndef SCHEMA_H
#define SCHEMA_H

#define _POSIX_C_SOURCE 200809L
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

//=============================================================================
// LENS_COPY: smart copy that handles both scalar and array types
// Uses _Generic to dispatch based on destination type
//=============================================================================

#define LENS_COPY(dst, src) \
    _Generic((dst), \
        char*: memcpy((dst), (src), sizeof(dst)), \
        default: (dst) = (src) \
    )

#endif // SCHEMA_H
