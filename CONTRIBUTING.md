# Redpanda Connector Certification

Redpanda Connect supports a wide array of connectors for integrating with popular data systems. While many are community-contributed, certified connectors are officially supported by Redpanda.  
This document outlines the criteria for certification, ensuring a great user experience and sustainable supportability, while continuing to welcome high-quality community contributions.

---

## 1. Certification Overview

To certify a connector, it must meet the following requirements:

### 1.1 Clear Documentation & Good UX

- **1.1.1** Concise, well-organized documentation with configuration examples.  
- **1.1.2** Includes expected usage patterns, troubleshooting guidance, and known pitfalls.  
- **1.1.3** UX should be intuitive and require minimal explanation. Follow a “don’t make me think” philosophy.

### 1.2 Observability & Debuggability

- **1.2.1** Exposes useful metrics for debugging that avoid excessive cardinality.  
- **1.2.2** Provides relevant logging to support troubleshooting. Unexpected behavior should emit warning or error logs. Normal operation should emit no logs.  
- **1.2.3** Known limitations and edge cases are documented.  
- **1.2.4** Strongly lints and validates user-provided configuration, clearly telling users of any problems.

### 1.3 Reliability & Testing

- **1.3.1** Code is idiomatic following Effective Go recommendations, is readable, and is consistent with the broader Redpanda Connect code base.  
- **1.3.2** Tests should cover end-to-end functionality and prove that the connector works across supported configurations.  
- **1.3.3** Integration tests verify core workflows and are runnable in CI.
- **1.3.4** Benchmarking covers **two distinct phases**, both of which are required:
  - **Local benchmarking (localhost):** unit and local integration benchmarks that run without external infrastructure, giving fast, repeatable feedback during development.
  - **Real-endpoint benchmarking:** benchmarks run against a real deployed server / real endpoint, exercising the connector against the actual target system rather than a local stand-in.
  - Follow the standard process, directory layout, and reporting requirements in [`docs/benchmarking.md`](docs/benchmarking.md); record results under [`docs/benchmark-results/`](docs/benchmark-results/).
- **1.3.5** Across both phases, benchmarks have been run at various throughput levels so that we can determine CPU and memory trendlines based on usage.
- **1.3.6** If a corresponding Kafka Connect connector exists, benchmarks have been run against it so we can compare it against our throughput and ensure Redpanda Connect's is comparable or better.

---

## 2. Connector Selection Criteria

When deciding which connectors to prioritize or certify, Redpanda considers:

### 2.1 Preferred Characteristics

- **2.1.1** Integrates well with Redpanda as a company.  
- **2.1.2** Represents widely used and recognized tools in the data engineering ecosystem.  
- **2.1.3** Is well documented and has an active, engaged user base.

### 2.2 Deprioritized Characteristics

- **2.2.1** Niche, outdated, or declining technologies.  
- **2.2.2** High barriers to testing (e.g., requires proprietary infrastructure).  
- **2.2.3** Fragile, costly, or hard to operate in real-world environments.

---

## 3. Implementation Standards

We hold certified connectors to a consistent engineering bar so that they are reliable, maintainable, and supportable.

### 3.1 Required Engineering Qualities

- **3.1.1** Connector code is either authored by Redpanda engineers or reviewed and scoped by Redpanda before community contribution (e.g., defined in a GitHub issue).  
- **3.1.2** Code adheres to standard Go practices: idiomatic, well-structured, self-documenting, and formatted with `gofumpt` (`task fmt`) so it stays consistent with the rest of the codebase.
- **3.1.3** Connectors are written **Go-first** — idiomatic Go, never a line-by-line port of an implementation from another ecosystem (e.g. Debezium for CDC). We build toward a goal: **supporting the target endpoint well**, where "well" is defined by the rest of this document — clear documentation and UX (§1.1), well-designed configuration knobs and validation (§1.2.4), observability (§1.2), and reliability (§1.3). A reference implementation may be consulted to understand the endpoint or protocol, but it is not a specification to replicate, and we are not bound to its abstractions, idioms, or naming. Design the connector for our users and our codebase, not for parity with another tool.  
- **3.1.4** The implementation is complete and correct, with no known bugs or missing core functionality.  
- **3.1.5** The codebase feels consistent with other Redpanda Connect connectors, avoiding bespoke or idiosyncratic implementations.  
- **3.1.6** Integration tests are easy to run locally and in CI environments, ideally with containerized dependencies.  
- **3.1.7** Supports live credential rotation (e.g., for tokens or certs) with no downtime where applicable.  
- **3.1.8** Has sufficient observability: logs, metrics, and tracing hooks as expected.

### 3.2 Anti-Patterns to Avoid

- **3.2.1** Incomplete implementations.  
- **3.2.2** Poor error handling or difficult-to-diagnose bugs.  
- **3.2.3** Unfamiliar or confusing UX patterns.  
- **3.2.4** Code that is difficult to test or maintain.  
- **3.2.5** Excessive resource usage (e.g., unnecessary goroutines, memory or CPU overhead).

### 3.3 Contribution Process & Change Size

- **3.3.1** Changes are split into reviewable units. As a rule, neither a single PR nor an individual commit should reach ~10K lines of **code that a reviewer must read** — this keeps changes reviewable. AI-generated code counts in full: it needs *more* review, not less, and the limit is not a license to autogenerate past it. Only content that isn't reviewed line-by-line is excluded — mechanically generated/derived files (codegen output, mocks, bundle imports), vendored code, lockfiles, and non-code such as documentation, skills, Terraform, templates, and test fixtures/data. When a large PR is genuinely unavoidable, say why in the description and point reviewers at what matters.  
- **3.3.2** Large features are broken into a series of smaller, self-contained PRs that can each be reviewed and reasoned about independently, rather than landed as one monolithic change.  

---

## 4. Client Library Evaluation

The connector’s reliability also depends on the underlying client library:

### 4.1 Preferred Traits

- **4.1.1** Maintained by the vendor of the target technology.  
- **4.1.2** Actively developed and well adopted in the Go ecosystem.  
- **4.1.3** Stable, performant, and well understood.  
- **4.1.4** Adheres to semantic versioning and is v1 or greater.

### 4.2 Red Flags

- **4.2.1** Outdated or inactive libraries.  
- **4.2.2** Known security issues or critical bugs.  
- **4.2.3** Poor runtime behavior: excessive goroutines, memory leaks, or non-linear scaling.
