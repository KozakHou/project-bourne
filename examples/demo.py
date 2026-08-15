"""A tiny deterministic experiment for Project Bourne's quick start."""

samples = (1.0, 2.0, 3.0, 4.0)
mean = sum(samples) / len(samples)

print(f"samples={len(samples)}")
print(f"mean={mean:.3f}")
