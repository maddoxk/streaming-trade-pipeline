# Results

Raw outputs of `scripts/e2e.py`, taken from the `e2e-results` artifact of GitHub Actions run
[41665836914](https://github.com/maddoxk/streaming-trade-pipeline/actions/runs/41665836914)
(job `e2e`, `ubuntu-latest`, 2 vCPU, image 20261101.186.1). The commands are in
`.github/workflows/ci.yml`:

```bash
python scripts/e2e.py --speedup 0  --max-disorder-ms 5000 --label burst     --results results/ci_burst.json
python scripts/e2e.py --speedup 30 --max-disorder-ms 5000 --label paced_30x --results results/ci_paced_30x.json
```

Each CI run overwrites these files in its own artifact; the files committed here are from the run above
and are one sample from a shared runner, not a statistical benchmark.
