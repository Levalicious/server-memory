{
  "targets": [
    {
      "target_name": "graphstore",
      "sources": [
        "native/memoryfile.c",
        "native/stringtable.c",
        "native/graph.c",
        "native/graphbind.c"
      ],
      "include_dirs": [
        "native"
      ],
      "cflags": ["-std=c11", "-Wall", "-Wextra", "-O2", "-fno-strict-aliasing", "-fno-math-errno"],
      "conditions": [
        ["OS=='linux'", {
          "defines": ["_GNU_SOURCE"],
          "libraries!": ["-lnode"]
        }],
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CFLAGS": ["-std=c11", "-Wall", "-Wextra", "-O2", "-fno-strict-aliasing", "-fno-math-errno"]
          }
        }]
      ]
    }
  ]
}
