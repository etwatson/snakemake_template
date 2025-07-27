# Modular Snakemake Pipeline Template (Annotated)

*A reusable, well‑documented scaffold for building robust, modular, and extensible Snakemake workflows.*

---

## 0) Goals, Philosophy, and When to Use This Template

This template is meant to:

* **Keep pipelines modular.** Each feature area lives in its own rule file and can be enabled/disabled via `config.yaml`.
* **Be easy to extend.** New tools = new rule files; minimal edits elsewhere.
* **Be reproducible.** Every tool runs in its own Conda env.
* **Be understandable.** Verbose comments explain how Snakemake resolves dependencies, how wildcards work, and how to run Bash/R/Python/Java steps.
* **Be safe by default.** Path‑based dependencies avoid fragile cross‑file rule references; outputs are plain strings to dodge common parser pitfalls.

> **Audience:** New and intermediate Snakemake users who want a production‑ready structure you can evolve over time.

---

## 1) Project Layout

```
my-pipeline/
├─ Snakefile                         # entry point
├─ config/
│  └─ config.yaml                    # global settings / samples / module toggles
├─ envs/                             # per-tool Conda envs
│  ├─ snakemake.yaml
│  ├─ toolA.yaml
│  ├─ toolB.yaml
│  └─ r_bio.yaml
├─ rules/                            # modular rule files (one per feature area)
│  ├─ core.smk                       # example: preprocessing/core steps
│  ├─ featureA.smk                   
│  ├─ featureB.smk                   
│  ├─ featureC.smk                   
│  └─ merge.smk                      # final collation and exports
├─ scripts/                          # user scripts called by rules
│  ├─ helper.R
│  ├─ helper.py
│  ├─ helper.sh
│  └─ Helper.java
└─ resources/                        # static files (HMMs, schemas, adapters, etc.)
```

**Why this structure?**

* Keeps concerns separated (rules vs. config vs. envs vs. scripts).
* Makes it trivial to swap modules in/out.
* Encourages per‑tool environments and explicit data flow.

---

## 2) `config/config.yaml` (Template)

```yaml
project: "demo_project"
output_dir: "results"
threads: 8

# Globally toggle modules
modules:
  core:       true   # required base steps
  featureA:   true   
  featureB:   true   
  featureC:   true   # e.g., HMM/domain tagging
  merge:      true   # produce final outputs

# Samples and their inputs
samples:
  SAMPLE1:
    fasta: data/genomes/SAMPLE1.fasta
    # optional inputs per module
    featureB_input: data/methyl/SAMPLE1.bed

  SAMPLE2:
    fasta: data/genomes/SAMPLE2.fasta
    featureB_input: data/methyl/SAMPLE2.bed

# Tool‑specific settings (safe defaults shown)
core:
  cpus: 4

featureA:
  cpus: 4

featureB:
  window: 500

featureC:
  hmm: resources/hmms/PF01535.hmm
  evalue: 1e-5

merge:
  export_formats: ["gff3", "gbk"]
```

**Key ideas**

* **Single source of truth:** Users edit only `config.yaml` to select inputs, parameters, and what modules run.
* **Module toggles:** `modules:<name>: true/false` allows you to ship a pipeline that gracefully disables components (with placeholders) without breaking dependencies.

---

## 3) `Snakefile` (Template)

```python
# Snakefile — minimal, durable entry point
configfile: "config/config.yaml"

OUTDIR   = config["output_dir"]
SAMPLES  = sorted(config.get("samples", {}).keys())

# Include *all* rule files. Each file decides internally whether to define
# real rules or light-weight placeholders based on config.modules.
include: "rules/core.smk"
include: "rules/featureA.smk"
include: "rules/featureB.smk"
include: "rules/featureC.smk"
include: "rules/merge.smk"

# Final targets: let the merge module define exactly which files are the final artifacts.
rule all:
    input:
        expand(f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gff3", sample=SAMPLES),
        expand(f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gbk",  sample=SAMPLES)
```

**Important:**

* The `Snakefile` stays tiny and stable. Modules own their own logic.
* Use **path-based dependencies** (i.e., rule `input:` refers to a concrete file path) so Snakemake can infer the DAG without cross‑referencing `rules.other_rule.output` across files.

---

## 4) Writing Modular Rule Files

Each `rules/*.smk` file:

1. Reads shared config keys.
2. Checks `modules.<name>` to decide whether to define **real** rules or **placeholder** rules.
3. Uses **plain strings** for `output:` (no lambdas or wrappers like `directory()` or `temp()` in outputs). Lambdas are allowed only in `input:`.
4. Uses **path-based inputs** to refer to files from earlier steps.
5. (Optionally) Provides **sentinel files** (e.g., `.done`) to represent directories as dependencies.

### 4.1 Example: `rules/core.smk`

```python
# rules/core.smk
OUTDIR = config["output_dir"]
MODS   = config.get("modules", {})
CORE   = config.get("core", {})
CPUS   = int(CORE.get("cpus", 4))

if not MODS.get("core", True):
    # Placeholders: create minimal artifacts so downstream rules have inputs
    rule core_clean:
        input:
            fasta = lambda wc: config["samples"][wc.sample]["fasta"]
        output:
            fa = f"{OUTDIR}/{{sample}}/core/00_clean/{{sample}}.clean.fa"
        shell:
            r"""
            mkdir -p $(dirname {output.fa})
            cp {input.fasta} {output.fa}
            """

    rule core_sort:
        input:
            fa = f"{OUTDIR}/{{sample}}/core/00_clean/{{sample}}.clean.fa"
        output:
            fa = f"{OUTDIR}/{{sample}}/core/01_sort/{{sample}}.sorted.fa"
        shell:
            r"""
            mkdir -p $(dirname {output.fa})
            cp {input.fa} {output.fa}
            """

else:
    # Real rules: call your actual tools here
    rule core_clean:
        conda: "envs/toolA.yaml"
        threads: 1
        input:
            fasta = lambda wc: config["samples"][wc.sample]["fasta"]
        output:
            fa = f"{OUTDIR}/{{sample}}/core/00_clean/{{sample}}.clean.fa"
        shell:
            r"""
            mkdir -p $(dirname {output.fa})
            toolA_clean -i {input.fasta} -o {output.fa}
            """

    rule core_sort:
        conda: "envs/toolA.yaml"
        threads: 1
        input:
            fa = f"{OUTDIR}/{{sample}}/core/00_clean/{{sample}}.clean.fa"
        output:
            fa = f"{OUTDIR}/{{sample}}/core/01_sort/{{sample}}.sorted.fa"
        shell:
            r"""
            mkdir -p $(dirname {output.fa})
            toolA_sort -i {input.fa} -o {output.fa}
            """
```

### 4.2 Example: `rules/featureA.smk` (depends on core)

```python
# rules/featureA.smk
OUTDIR = config["output_dir"]
MODS   = config.get("modules", {})
A      = config.get("featureA", {})

if not MODS.get("featureA", True):
    rule featureA_run:
        input:
            fa = f"{OUTDIR}/{{sample}}/core/01_sort/{{sample}}.sorted.fa"
        output:
            gff = f"{OUTDIR}/{{sample}}/featureA/{{sample}}.A.gff3",
            lib = f"{OUTDIR}/{{sample}}/featureA/{{sample}}.A.lib.fa"
        shell:
            r"""
            mkdir -p $(dirname {output.gff})
            echo "##gff-version 3" > {output.gff}
            : > {output.lib}
            """
else:
    rule featureA_run:
        conda: "envs/toolB.yaml"
        threads: int(A.get("cpus", 4))
        input:
            fa = f"{OUTDIR}/{{sample}}/core/01_sort/{{sample}}.sorted.fa"
        output:
            gff = f"{OUTDIR}/{{sample}}/featureA/{{sample}}.A.gff3",
            lib = f"{OUTDIR}/{{sample}}/featureA/{{sample}}.A.lib.fa"
        params:
            outdir = lambda wc: f"{OUTDIR}/{wc.sample}/featureA"
        shell:
            r"""
            mkdir -p {params.outdir}
            toolB -g {input.fa} -o {params.outdir} --threads {threads}
            # copy normalized outputs
            cp {params.outdir}/calls.gff3 {output.gff}
            cp {params.outdir}/consensus.fa {output.lib}
            """
```

### 4.3 Example: `rules/featureB.smk` (consumes external per‑sample files)

```python
# rules/featureB.smk
OUTDIR = config["output_dir"]
MODS   = config.get("modules", {})
B      = config.get("featureB", {})

if not MODS.get("featureB", True):
    rule featureB_cluster:
        input:
            bed = lambda wc: str(config["samples"][wc.sample].get("featureB_input", ""))
        output:
            gff = f"{OUTDIR}/{{sample}}/featureB/{{sample}}.B.gff3"
        shell:
            r"""
            mkdir -p $(dirname {output.gff})
            echo "##gff-version 3" > {output.gff}
            """
else:
    rule featureB_cluster:
        conda: "envs/r_bio.yaml"
        input:
            bed = lambda wc: str(config["samples"][wc.sample]["featureB_input"])
        output:
            gff = f"{OUTDIR}/{{sample}}/featureB/{{sample}}.B.gff3"
        params:
            window = int(B.get("window", 500))
        shell:
            r"""
            mkdir -p $(dirname {output.gff})
            Rscript scripts/helper.R \
              --bed {input.bed} \
              --window {params.window} \
              --out {output.gff}
            """
```

### 4.4 Example: `rules/featureC.smk` (HMM/domain tagging on proteins)

```python
# rules/featureC.smk
OUTDIR = config["output_dir"]
MODS   = config.get("modules", {})
C      = config.get("featureC", {})

HMM    = str(C.get("hmm", "")).strip()
EVALUE = str(C.get("evalue", "1e-5")).strip()

if not MODS.get("featureC", True):
    rule featureC_search:
        input:
            proteins = f"{OUTDIR}/{{sample}}/core/02_predict/{{sample}}.proteins.fa"
        output:
            tbl = f"{OUTDIR}/{{sample}}/featureC/{{sample}}.C.tbl"
        shell:
            r"""
            mkdir -p $(dirname {output.tbl})
            : > {output.tbl}
            """
else:
    rule featureC_search:
        conda: "envs/toolA.yaml"   # or hmmer.yaml
        threads: 4
        input:
            proteins = f"{OUTDIR}/{{sample}}/core/02_predict/{{sample}}.proteins.fa"
        output:
            tbl = f"{OUTDIR}/{{sample}}/featureC/{{sample}}.C.tbl"
        params:
            hmm = HMM,
            e   = EVALUE
        shell:
            r"""
            mkdir -p $(dirname {output.tbl})
            hmmsearch --cpu {threads} -E {params.e} --tblout {output.tbl} {params.hmm} {input.proteins}
            """
```

### 4.5 Example: `rules/merge.smk` (final collation)

```python
# rules/merge.smk
OUTDIR = config["output_dir"]
MODS   = config.get("modules", {})

if not MODS.get("merge", True):
    rule final_copy:
        input:
            gff = f"{OUTDIR}/{{sample}}/core/03_annot/{{sample}}.core.gff3"
        output:
            gff = f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gff3",
            gbk = f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gbk"
        shell:
            r"""
            mkdir -p $(dirname {output.gff})
            cp {input.gff} {output.gff} || echo "##gff-version 3" > {output.gff}
            : > {output.gbk}
            """
else:
    rule merge_tracks:
        conda: "envs/r_bio.yaml"
        input:
            core = f"{OUTDIR}/{{sample}}/core/03_annot/{{sample}}.core.gff3",
            A    = f"{OUTDIR}/{{sample}}/featureA/{{sample}}.A.gff3",
            B    = f"{OUTDIR}/{{sample}}/featureB/{{sample}}.B.gff3",
            C    = f"{OUTDIR}/{{sample}}/featureC/{{sample}}.C.tbl"
        output:
            gff = f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gff3"
        shell:
            r"""
            mkdir -p $(dirname {output.gff})
            Rscript scripts/helper.R --merge \
              --core {input.core} --A {input.A} --B {input.B} --C {input.C} \
              --out {output.gff}
            """

    rule export_genbank:
        conda: "envs/toolA.yaml"   # env that contains your exporter
        input:
            gff   = f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gff3",
            fasta = f"{OUTDIR}/{{sample}}/core/01_sort/{{sample}}.sorted.fa"
        output:
            gbk = f"{OUTDIR}/{{sample}}/final/{{sample}}.final.gbk"
        shell:
            r"""
            exporter_tool --gff {input.gff} --fasta {input.fasta} --out {output.gbk}
            """
```

---

## 5) How Snakemake Resolves the DAG (and Why This Template Works)

* **Path-based inputs drive the graph.** Each rule’s `input:` lists concrete file paths. Snakemake searches for rules that produce those paths, chaining requirements until it reaches source files.
* **Wildcards (`{sample}`)** bind variables in file patterns. If a rule outputs `results/{sample}/x.txt` and `sample=H97`, Snakemake will concretize that path as `results/H97/x.txt`.
* **No cross‑file rule references.** Instead of `input: x = rules.other.output.y`, we use the *paths those rules produce*. This makes include order irrelevant and keeps modules self‑contained.
* **Outputs are plain strings.** Avoid functions/wrappers in `output:`; many Snakemake versions allow callables only in `input:`. If you need to represent a directory dependency, use a **sentinel file** (e.g., `.done`).

---

## 6) Wildcards 101 (With Examples)

Wildcards are placeholders in filenames:

```python
output:
    gff = f"results/{{sample}}/annot/{{sample}}.gff3"
```

* When you request `results/H97/annot/H97.gff3`, `sample` binds to `H97` and flows through inputs.
* **Rules with multiple wildcards** must ensure that all inputs can be inferred from outputs (or you must provide a mapping function for `input:`).
* Prefer **one wildcard per axis** (e.g., `{sample}`, optionally `{chr}`) to avoid ambiguity.

**Dynamic inputs** (allowed):

```python
input:
    fasta = lambda wc: config["samples"][wc.sample]["fasta"]
```

**Dynamic outputs** (avoid): Keep outputs as literal strings with wildcards.

---

## 7) Calling Bash, R, Python, or Java from Rules

Snakemake supports multiple integration patterns. Choose the one that maximizes clarity and reproducibility.

### 7.1 Bash via `shell:`

```python
rule run_tool:
    conda: "envs/tool.yaml"
    input:  tsv = "data/input.tsv"
    output: out = "results/{sample}/tool/out.txt"
    shell:
        r"""
        tool --in {input.tsv} --sample {wildcards.sample} > {output.out}
        """
```

### 7.2 R via `Rscript` (recommended)

```python
rule summarize:
    conda: "envs/r_bio.yaml"
    input:  gff = "results/{sample}/final/{sample}.final.gff3"
    output: pdf = "results/{sample}/qc/{sample}.summary.pdf"
    shell:
        r"""
        Rscript scripts/helper.R --gff {input.gff} --out {output.pdf}
        """
```

### 7.3 Python via `python` or `scripts:`

**Shell + python:**

```python
rule py_helper:
    conda: "envs/py.yaml"
    input:  x = "data/x.tsv"
    output: y = "results/{sample}/y.tsv"
    shell:
        r"""
        python scripts/helper.py --in {input.x} --out {output.y}
        """
```

**Inline `run:` blocks** (simple transformations):

```python
rule inline_py:
    input:  x = "data/x.tsv"
    output: y = "results/{sample}/y.tsv"
    run:
        with open(input.x) as fin, open(output.y, 'w') as fout:
            for line in fin:
                fout.write(line)
```

### 7.4 Java via `java` (in `shell:`)

```python
rule run_java:
    conda: "envs/java.yaml"
    input:  fa = "results/{sample}/core/01_sort/{sample}.sorted.fa"
    output: vcf = "results/{sample}/java/{sample}.calls.vcf"
    params:
        jar = "scripts/Helper.jar"
    shell:
        r"""
        java -Xmx4g -jar {params.jar} --in {input.fa} --out {output.vcf}
        """
```

**General tips**

* Put each tool in its own **Conda env** (`envs/*.yaml`).
* Pass parameters in `params:`; pass files in `input:`/`output:`.
* Use `{wildcards.sample}` to thread the sample name through commands.

---

## 8) Conda Environments (Template Files)

### 8.1 `envs/snakemake.yaml`

```yaml
name: snakemake
channels: [conda-forge, bioconda, defaults]
dependencies:
  - snakemake>=7
```

### 8.2 `envs/r_bio.yaml`

```yaml
name: r_bio
channels: [conda-forge, bioconda, defaults]
dependencies:
  - r-base>=4.2
  - r-data.table
  - r-stringr
  - r-ggplot2
  - bioconductor-genomicranges
  - bioconductor-rtracklayer
  - bioconductor-biostrings
  - bioconductor-iranges
  - r-optparse
```

*(Add tool‑specific envs as needed: `toolA.yaml`, `toolB.yaml`, `hmmer.yaml`, `java.yaml`, etc.)*

---

## 9) Common Design Patterns (and Why)

* **Path‑based dependencies:** Inputs refer to concrete paths, so Snakemake can infer the DAG without fragile `rules.other.output` references.
* **Placeholders for disabled modules:** When `modules.X: false`, define stub rules that create empty/identity outputs at the expected paths. Downstream rules still work.
* **Sentinel files** for directory dependencies: produce `.done` files in work dirs instead of using `directory()` in `output:`; it’s compatible across Snakemake versions and plays nicely with cleanup.
* **Conditional flags in `params`:** Build command‑line flags (e.g., for optional evidence) in `params:` and interpolate into `shell:`. Keep `input:` strictly to files.
* **One wildcard per axis:** Simplifies reasoning and prevents ambiguous expansions.

---

## 10) Running, Inspecting, and Debugging

**Dry run:**

```bash
snakemake -n -p              # show commands without running
```

**Use Conda and parallelism:**

```bash
snakemake --use-conda --conda-frontend mamba -j 8 -p
```

**See the DAG:**

```bash
snakemake --dag | dot -Tpdf > dag.pdf
```

**Rerun a specific target:**

```bash
snakemake results/SAMPLE1/final/SAMPLE1.final.gff3 --use-conda -j 4 -p
```

**Force rerun a rule:**

```bash
snakemake -R featureA_run --use-conda -j 4 -p
```

**Troubleshoot missing files:**

* Check that **every `output:` is a plain string** (no functions/wrappers).
* Verify config paths exist for source inputs.
* Simplify: comment out modules and add back one at a time.

---

## 11) Extending the Template

* **Add a new module:** Create `rules/newFeature.smk`, add a toggle under `modules:` in `config.yaml`, and include the file in `Snakefile`.
* **Per‑sample toggles:** Put a `modules:` block inside each sample in `config.yaml`, and read those flags inside the module; or have the module check for required per‑sample inputs (and emit a placeholder if missing).
* **Reports & QC:** Add a `rules/report.smk` that collects metrics and renders an HTML report (e.g., Rmarkdown or Python + Jinja2) using all final artifacts.

---

## 12) Script Templates

### 12.1 `scripts/helper.R`

```r
#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(optparse); library(data.table); library(GenomicRanges); library(rtracklayer)
})

opt <- OptionParser() |>
  add_option("--bed",  type="character", default=NA) |>
  add_option("--window", type="integer", default=500) |>
  add_option("--merge", action="store_true", default=FALSE) |>
  add_option("--core", type="character") |>
  add_option("--A", type="character") |>
  add_option("--B", type="character") |>
  add_option("--C", type="character") |>
  add_option("--out", type="character") |>
  parse_args()

if (!is.na(opt$bed)) {
  dt <- fread(opt$bed, header=FALSE)
  setnames(dt, 1:3, c("chr","start","end"))
  gr <- GRanges(seqnames=dt$chr, ranges=IRanges(dt$start+1, dt$end))
  gr <- reduce(gr, min.gapwidth = opt$window)
  mcols(gr)$type <- "cluster"
  export(gr, opt$out, format="GFF3")
} else if (opt$merge) {
  gr <- c(import(opt$core), import(opt$A), import(opt$B))
  # C could be a table; parse and annotate as needed
  export(sort(gr), opt$out, format="GFF3")
}
```

### 12.2 `scripts/helper.py`

```python
#!/usr/bin/env python3
import argparse, csv
ap = argparse.ArgumentParser()
ap.add_argument('--in', dest='inp', required=True)
ap.add_argument('--out', dest='out', required=True)
args = ap.parse_args()

with open(args.inp) as fin, open(args.out, 'w') as fout:
    for line in fin:
        fout.write(line)
```

### 12.3 `scripts/helper.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail
inp="$1"; out="$2"
cp "$inp" "$out"
```

### 12.4 `scripts/Helper.java`

```java
import java.nio.file.*; import java.io.*;
public class Helper {
  public static void main(String[] args) throws Exception {
    Path in = Paths.get(args[0]);
    Path out = Paths.get(args[1]);
    Files.copy(in, out, StandardCopyOption.REPLACE_EXISTING);
  }
}
```

---

## 13) Checklist for New Pipelines

* [ ] Define inputs in `config.yaml` under `samples:`.
* [ ] Choose module toggles in `modules:`.
* [ ] Create/adjust per‑tool Conda envs in `envs/`.
* [ ] Write/modify `rules/*.smk` with path‑based `input:` and literal string `output:`.
* [ ] Add any new scripts to `scripts/` and make them executable (`chmod +x`).
* [ ] Dry run (`-n -p`), inspect DAG, then run with `--use-conda`.

---

## 14) Notes on Good Practices (Recap)

* Keep the **Snakefile** tiny; put logic in modular rule files.
* Favor **path‑based DAG construction** over cross‑file `rules.x` references.
* Use **Conda per tool** for reproducibility.
* Keep `output:` **plain strings**; if you need directories, create **sentinels**.
* Gate modules with `config.modules` and provide **placeholders** when off.
* Start small; add modules one by one; dry‑run often and version‑control your pipeline.

---

*Happy building! Duplicate this template, rename modules to match your domain (annotation, alignment, QC, etc.), and you’ve got a durable base for future Snakemake projects.*
