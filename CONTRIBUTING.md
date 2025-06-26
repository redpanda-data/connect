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
- **3.1.2** Code adheres to standard Go practices: idiomatic, well-structured, and self-documenting.  
- **3.1.3** The implementation is complete and correct, with no known bugs or missing core functionality.  
- **3.1.4** The codebase feels consistent with other Redpanda Connect connectors, avoiding bespoke or idiosyncratic implementations.  
- **3.1.5** Integration tests are easy to run locally and in CI environments, ideally with containerized dependencies.  
- **3.1.6** Supports live credential rotation (e.g., for tokens or certs) with no downtime where applicable.  
- **3.1.7** Has sufficient observability: logs, metrics, and tracing hooks as expected.

### 3.2 Anti-Patterns to Avoid

- **3.2.1** Incomplete implementations.  
- **3.2.2** Poor error handling or difficult-to-diagnose bugs.  
- **3.2.3** Unfamiliar or confusing UX patterns.  
- **3.2.4** Code that is difficult to test or maintain.  
- **3.2.5** Excessive resource usage (e.g., unnecessary goroutines, memory or CPU overhead).

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
