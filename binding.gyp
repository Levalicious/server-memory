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
      "defines": ["_GNU_SOURCE"]
    }
  ]
}
