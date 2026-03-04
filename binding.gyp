{
  "targets": [
    {
      "target_name": "memoryfile",
      "sources": [
        "native/memoryfile.c",
        "native/binding.c"
      ],
      "include_dirs": [
        "native"
      ],
      "cflags": ["-std=c11", "-Wall", "-Wextra", "-O2"],
      "conditions": [
        ["OS=='linux'", {
          "defines": ["_GNU_SOURCE"]
        }],
        ["OS=='mac'", {
          "xcode_settings": {
            "OTHER_CFLAGS": ["-std=c11", "-Wall", "-Wextra", "-O2"]
          }
        }]
      ]
    }
  ]
}
