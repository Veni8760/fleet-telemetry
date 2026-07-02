# Lessons

- **Don't reinvent stdlib.** Wrote `atoi`/`atof` wrappers around `strconv.Atoi`/`strconv.ParseFloat`
  — a C-name shadow of the stdlib. Before writing any helper, check the ladder: does stdlib/builtin
  already do it? Inline the stdlib call. Also: Go 1.21+ has builtin `min`/`max` — no `math.Max`/`math.Min`.
