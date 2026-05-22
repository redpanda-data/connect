# Kafka Connect head-to-head — Plan 1: Redpanda + KC infra (no behavior change)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a shared Redpanda cluster in the bench VPC, install Kafka Connect (KC) + Debezium/Confluent/Iceberg/Aiven plugins on the runner EC2 with a long-lived single-worker process, and add a broker-side metrics scraper to each sweep point. **No bench behavior change** — Connect's pipeline still writes to `drop`; the new broker metric is uploaded to S3 but ignored by reporting. Plans 2/3/4 build on top.

**Architecture:** New `terraform/modules/redpanda/` provisions 3× `im4gn.2xlarge` brokers with NVMe instance-store data, static private IPs (deterministic seed list), and an in-VPC-only listener. Composed into `terraform/shared/redpanda.tf`. Runner EC2's cloud-init grows a KC install step (Corretto 21, AK 3.8 tarball, plugin JARs, systemd unit). Matrix runner's bench script gains a third subshell that scrapes Redpanda's admin metrics endpoint every 10s and uploads the framed dump to S3 alongside the existing prom file.

**Tech Stack:** Terraform (AWS), AL2023 arm64, Redpanda 24.x package via dnf, OpenJDK 21 (Amazon Corretto), Apache Kafka 3.8.x, Debezium 2.7.3.Final, Iceberg KC 1.7.x, Confluent S3 Sink 10.5.x, Aiven JDBC Sink 6.10.x, Go (existing runner package).

**Spec:** [`docs/superpowers/specs/2026-05-22-kafka-connect-comparison-design.md`](../specs/2026-05-22-kafka-connect-comparison-design.md)

---

## File Structure

**New files:**

- `benchmarking/aws/terraform/modules/redpanda/main.tf` — broker module composition (provider passthrough only; resources split across the next two files).
- `benchmarking/aws/terraform/modules/redpanda/variables.tf` — `cluster_size` (default 3), `instance_type` (default `im4gn.2xlarge`), `vpc_id`, `subnet_ids`, `broker_ips` (list of 3 static IPs from the private subnet CIDR), `iam_instance_profile`, `name_prefix`, `allowed_client_sgs` (list of SG IDs).
- `benchmarking/aws/terraform/modules/redpanda/security.tf` — broker SG (Kafka 9092, admin/metrics 9644, RPC 33145, prometheus 9644). Ingress from `var.allowed_client_sgs` on Kafka+admin, broker-to-broker on RPC.
- `benchmarking/aws/terraform/modules/redpanda/instances.tf` — 3× `aws_instance` resources at the static IPs, AL2023 arm64 AMI, instance-store-backed, root EBS 100GB gp3.
- `benchmarking/aws/terraform/modules/redpanda/outputs.tf` — `broker_endpoints` (comma-separated `host:9092` list), `metrics_endpoint` (first broker's `host:9644`), `broker_sg_id` (for cross-stack ingress).
- `benchmarking/aws/terraform/modules/redpanda/user-data.tftpl` — per-broker cloud-init template: format NVMe, install Redpanda, write seed config with `node_id` and `seed_servers`, enable systemd unit.
- `benchmarking/aws/terraform/shared/redpanda.tf` — composes the module.

**Modified files:**

- `benchmarking/aws/terraform/shared/runner.tf` — switch `local.cloud_init` to a `templatefile()` rendering of a new user-data template that ALSO installs KC + plugins + systemd unit. Pass the Redpanda broker list in.
- `benchmarking/aws/terraform/shared/runner-user-data.tftpl` — new file holding the full runner user-data (extracted from inline `<<-EOT`).
- `benchmarking/aws/terraform/shared/outputs.tf` — re-export `redpanda_broker_endpoints` and `redpanda_metrics_endpoint`.
- `benchmarking/aws/terraform/shared/security.tf` — add SG rule resources allowing `runner_sg` and `load_gen_sg` to reach the broker SG on Kafka + admin ports.
- `benchmarking/aws/terraform/shared/variables.tf` — new `redpanda_broker_ips`, `redpanda_instance_type` (with defaults).
- `benchmarking/aws/terraform/shared/vpc.tf` — pin a `/26` reserved range out of the existing private subnet for static broker IPs (no resource change; just a comment + a `locals` block expressing the addresses).
- `benchmarking/aws/runner/matrix.go` — `MatrixRunner` gains `RedpandaMetricsEndpoint string`. `benchScriptArgs` gains `RedpandaMetricsEndpoint string`. `renderBenchScript` appends a third subshell that scrapes the endpoint every 10s and uploads to S3 as `runs/<sess>/redpanda-<vcpu>.txt`.
- `benchmarking/aws/runner/matrix_test.go` — assert the new subshell is rendered when the endpoint is set, and is omitted when empty.
- `benchmarking/aws/runner/main.go` — read `redpanda_metrics_endpoint` from the terraform outputs map and pass it into `MatrixRunner`.

**Why this split:** The Redpanda module is reusable (a future scenario might want a dedicated broker), so it lives in `modules/` mirroring `modules/rds-postgres/` and `modules/rds-mysql/`. Runner user-data has grown beyond a tractable inline heredoc; extracting it to a template file in this plan keeps Plan 2's pipeline-output change (also touching runner user-data path) ergonomic. The bench-script change is intentionally minimal — just a third subshell — because Plan 3 owns the parsing/reporting integration.

---

## Task ordering

Phase A (TF: tasks 1–8) gets the broker cluster running and reachable. Phase B (tasks 9–10) installs KC on the runner. Phase C (tasks 11–12) wires the scraper into the bench script. Phase D (task 13) is the AWS smoke test.

Apply order matters: the broker module + runner KC install can be validated independently (`terraform validate` on the shared stack) but real verification needs an actual `apply`, deferred to task 13.

---

### Task 1: Create the Redpanda TF module skeleton

**Files:**
- Create: `benchmarking/aws/terraform/modules/redpanda/main.tf`
- Create: `benchmarking/aws/terraform/modules/redpanda/variables.tf`
- Create: `benchmarking/aws/terraform/modules/redpanda/outputs.tf`

- [ ] **Step 1: Create `main.tf` with provider block**

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.70" }
  }
}

data "aws_ssm_parameter" "al2023_arm64_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64"
}
```

- [ ] **Step 2: Create `variables.tf`**

```hcl
variable "name_prefix" {
  description = "Resource name prefix (matches shared stack)."
  type        = string
}

variable "cluster_size" {
  description = "Number of brokers. Static list of broker_ips must match this length."
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "EC2 instance type per broker. Default Graviton ARM with NVMe instance store."
  type        = string
  default     = "im4gn.2xlarge"
}

variable "vpc_id" {
  description = "VPC the brokers live in."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs for broker placement (length must >= cluster_size)."
  type        = list(string)
}

variable "broker_ips" {
  description = "Static private IPs for brokers, one per broker. Must be within the matching subnet."
  type        = list(string)
}

variable "iam_instance_profile" {
  description = "IAM instance profile name attached to each broker (for SSM access)."
  type        = string
}

variable "allowed_client_sgs" {
  description = "Security groups permitted to reach Kafka (9092) and the admin/metrics endpoint (9644)."
  type        = list(string)
}
```

- [ ] **Step 3: Create empty `outputs.tf` (filled in task 5)**

```hcl
# Outputs added in task 5 once instances exist.
```

- [ ] **Step 4: Validate the module compiles**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/terraform/modules/redpanda/
git commit -m "feat(bench/aws/redpanda): module skeleton + variables"
```

---

### Task 2: Redpanda broker security group

**Files:**
- Create: `benchmarking/aws/terraform/modules/redpanda/security.tf`

- [ ] **Step 1: Write `security.tf`**

```hcl
resource "aws_security_group" "broker" {
  name        = "${var.name_prefix}-redpanda"
  description = "Redpanda broker cluster - kafka, admin, RPC"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Kafka API from allowed clients
resource "aws_security_group_rule" "broker_kafka_ingress" {
  count                    = length(var.allowed_client_sgs)
  type                     = "ingress"
  from_port                = 9092
  to_port                  = 9092
  protocol                 = "tcp"
  source_security_group_id = var.allowed_client_sgs[count.index]
  security_group_id        = aws_security_group.broker.id
  description              = "Kafka from client SG"
}

# Admin / Prometheus metrics from allowed clients
resource "aws_security_group_rule" "broker_admin_ingress" {
  count                    = length(var.allowed_client_sgs)
  type                     = "ingress"
  from_port                = 9644
  to_port                  = 9644
  protocol                 = "tcp"
  source_security_group_id = var.allowed_client_sgs[count.index]
  security_group_id        = aws_security_group.broker.id
  description              = "Admin/metrics from client SG"
}

# Broker-to-broker RPC (raft)
resource "aws_security_group_rule" "broker_rpc_self" {
  type                     = "ingress"
  from_port                = 33145
  to_port                  = 33145
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.broker.id
  security_group_id        = aws_security_group.broker.id
  description              = "Broker RPC (raft)"
}
```

- [ ] **Step 2: Validate**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/terraform/modules/redpanda/security.tf
git commit -m "feat(bench/aws/redpanda): broker security group + client ingress rules"
```

---

### Task 3: Redpanda broker EC2 instances with static IPs

**Files:**
- Create: `benchmarking/aws/terraform/modules/redpanda/instances.tf`

- [ ] **Step 1: Write `instances.tf`**

```hcl
resource "aws_instance" "broker" {
  count                  = var.cluster_size
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.instance_type
  subnet_id              = var.subnet_ids[count.index % length(var.subnet_ids)]
  private_ip             = var.broker_ips[count.index]
  vpc_security_group_ids = [aws_security_group.broker.id]
  iam_instance_profile   = var.iam_instance_profile

  user_data = templatefile("${path.module}/user-data.tftpl", {
    node_id         = count.index
    self_ip         = var.broker_ips[count.index]
    seed_servers    = join(",", [for ip in var.broker_ips : "${ip}:33145"])
    advertised_kafka = "${var.broker_ips[count.index]}:9092"
    advertised_rpc  = "${var.broker_ips[count.index]}:33145"
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 250
    iops        = 3000
  }

  tags = {
    Name = "${var.name_prefix}-redpanda-${count.index}"
    Role = "redpanda-broker"
  }
}
```

- [ ] **Step 2: Create placeholder `user-data.tftpl` so terraform validate is happy**

```bash
cat > benchmarking/aws/terraform/modules/redpanda/user-data.tftpl <<'EOF'
#cloud-config
# Replaced in task 4.
runcmd:
  - echo "node_id=${node_id} self_ip=${self_ip} seeds=${seed_servers}"
EOF
```

- [ ] **Step 3: Validate**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/terraform/modules/redpanda/instances.tf benchmarking/aws/terraform/modules/redpanda/user-data.tftpl
git commit -m "feat(bench/aws/redpanda): broker instances with static IPs + placeholder cloud-init"
```

---

### Task 4: Redpanda cluster bootstrap (cloud-init)

**Files:**
- Modify: `benchmarking/aws/terraform/modules/redpanda/user-data.tftpl`

- [ ] **Step 1: Replace the placeholder with the real bootstrap**

```bash
cat > benchmarking/aws/terraform/modules/redpanda/user-data.tftpl <<'EOF'
#cloud-config
package_update: true
runcmd:
  # Format and mount the NVMe instance store (im4gn family exposes 1× /dev/nvme1n1).
  - mkfs.xfs -f /dev/nvme1n1
  - mkdir -p /var/lib/redpanda/data
  - mount /dev/nvme1n1 /var/lib/redpanda/data
  - echo "/dev/nvme1n1 /var/lib/redpanda/data xfs defaults,noatime 0 0" >> /etc/fstab

  # Install Redpanda from the official rpm setup script.
  - curl -sSL https://packages.redpanda.com/public/redpanda/setup.rpm.sh | bash
  - dnf install -y redpanda

  # Ensure the redpanda user owns the data dir.
  - chown -R redpanda:redpanda /var/lib/redpanda/data

  # Write the broker config. Static IPs let us seed deterministically.
  - |
    cat > /etc/redpanda/redpanda.yaml <<RP
    redpanda:
      data_directory: /var/lib/redpanda/data
      node_id: ${node_id}
      empty_seed_starts_cluster: false
      seed_servers:
%{ for ip in split(",", seed_servers) ~}
        - host: { address: "$(echo ${ip} | cut -d: -f1)", port: 33145 }
%{ endfor ~}
      rpc_server:
        address: ${self_ip}
        port: 33145
      kafka_api:
        - address: ${self_ip}
          port: 9092
      admin:
        - address: 0.0.0.0
          port: 9644
      advertised_kafka_api:
        - address: ${advertised_kafka%:*}
          port: 9092
      advertised_rpc_api:
        address: ${advertised_rpc%:*}
        port: 33145
    pandaproxy: {}
    schema_registry: {}
    RP
  - chown redpanda:redpanda /etc/redpanda/redpanda.yaml

  # Enable + start.
  - systemctl daemon-reload
  - systemctl enable --now redpanda
EOF
```

- [ ] **Step 2: Validate (template syntax errors surface here)**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Sanity-check template rendering with terraform console**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform console <<'EOF'
templatefile("./user-data.tftpl", {
  node_id          = 0,
  self_ip          = "10.42.10.10",
  seed_servers     = "10.42.10.10:33145,10.42.10.11:33145,10.0.10.12:33145",
  advertised_kafka = "10.42.10.10:9092",
  advertised_rpc   = "10.42.10.10:33145",
})
EOF
```

Expected: prints the rendered YAML cloud-init, with three `- host: { address: "10.42.10.10", port: 33145 }` (etc.) entries under `seed_servers:`.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/terraform/modules/redpanda/user-data.tftpl
git commit -m "feat(bench/aws/redpanda): cloud-init bootstrap (NVMe mount, dnf install, seed cluster)"
```

---

### Task 5: Redpanda module outputs

**Files:**
- Modify: `benchmarking/aws/terraform/modules/redpanda/outputs.tf`

- [ ] **Step 1: Write the outputs**

```hcl
output "broker_endpoints" {
  description = "Comma-separated host:9092 list, suitable as Kafka bootstrap.servers."
  value       = join(",", [for ip in var.broker_ips : "${ip}:9092"])
}

output "metrics_endpoint" {
  description = "First broker's host:9644 — scraping point for Redpanda Prometheus metrics."
  value       = "${var.broker_ips[0]}:9644"
}

output "broker_sg_id" {
  description = "Broker security group ID — for downstream ingress rules from new client SGs."
  value       = aws_security_group.broker.id
}
```

- [ ] **Step 2: Validate**

Run:
```bash
cd benchmarking/aws/terraform/modules/redpanda
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Commit**

```bash
git add benchmarking/aws/terraform/modules/redpanda/outputs.tf
git commit -m "feat(bench/aws/redpanda): module outputs (brokers, metrics, SG ID)"
```

---

### Task 6: Compose Redpanda into the shared stack

**Files:**
- Modify: `benchmarking/aws/terraform/shared/variables.tf`
- Create: `benchmarking/aws/terraform/shared/redpanda.tf`
- Modify: `benchmarking/aws/terraform/shared/outputs.tf`

- [ ] **Step 1: Add new variables**

Append to `benchmarking/aws/terraform/shared/variables.tf`:

```hcl
variable "redpanda_instance_type" {
  description = "EC2 instance type per Redpanda broker."
  type        = string
  default     = "im4gn.2xlarge"
}

variable "redpanda_broker_ips" {
  description = "Static private IPs for Redpanda brokers (must fall inside the private subnets' CIDRs)."
  type        = list(string)
  default     = ["10.42.10.10", "10.42.11.10", "10.42.10.11"]
}
```

(The default IPs reflect the existing VPC CIDR `10.42.0.0/16` with private subnets at `10.42.10.0/24` and `10.42.11.0/24` — derived from `cidrsubnet(var.vpc_cidr, 8, 10 + count.index)` in `vpc.tf` against `var.vpc_cidr = "10.42.0.0/16"` (see `terraform/shared/variables.tf`). Brokers 0 and 2 land in the first private subnet; broker 1 in the second.)

- [ ] **Step 2: Create `redpanda.tf`**

```hcl
module "redpanda" {
  source = "../modules/redpanda"

  name_prefix          = local.name_prefix
  cluster_size         = 3
  instance_type        = var.redpanda_instance_type
  vpc_id               = aws_vpc.main.id
  subnet_ids           = aws_subnet.private[*].id
  broker_ips           = var.redpanda_broker_ips
  iam_instance_profile = aws_iam_instance_profile.bench_host.name
  allowed_client_sgs   = [aws_security_group.runner.id, aws_security_group.load_gen.id]
}
```

- [ ] **Step 3: Re-export module outputs from the shared stack**

Append to `benchmarking/aws/terraform/shared/outputs.tf`:

```hcl
output "redpanda_broker_endpoints" { value = module.redpanda.broker_endpoints }
output "redpanda_metrics_endpoint" { value = module.redpanda.metrics_endpoint }
output "redpanda_broker_sg_id"     { value = module.redpanda.broker_sg_id }
```

- [ ] **Step 4: Validate**

Run:
```bash
cd benchmarking/aws/terraform/shared
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/terraform/shared/redpanda.tf benchmarking/aws/terraform/shared/variables.tf benchmarking/aws/terraform/shared/outputs.tf
git commit -m "feat(bench/aws): compose Redpanda module into shared stack"
```

---

### Task 7: Extract runner cloud-init to a template file

**Files:**
- Create: `benchmarking/aws/terraform/shared/runner-user-data.tftpl`
- Modify: `benchmarking/aws/terraform/shared/runner.tf`

This refactor is the same logic as today, just out-of-band — so the next task can layer KC install on top without diff churn.

- [ ] **Step 1: Create `runner-user-data.tftpl` with the existing logic**

```bash
cat > benchmarking/aws/terraform/shared/runner-user-data.tftpl <<'EOF'
#cloud-config
package_update: true
packages:
  - postgresql15
  - mariadb1011
  - jq
write_files:
  - path: /opt/bench/.gitkeep
    content: ""
runcmd:
  - mkdir -p /opt/bench
  - chmod 0755 /opt/bench
EOF
```

- [ ] **Step 2: Replace `local.cloud_init` in `runner.tf` with a `templatefile()` call**

Replace this block:

```hcl
locals {
  cloud_init = <<-EOT
    #cloud-config
    ...
  EOT
}

resource "aws_instance" "runner" {
  ...
  user_data = local.cloud_init
  ...
}

resource "aws_instance" "load_gen" {
  ...
  user_data = local.cloud_init
  ...
}
```

with:

```hcl
locals {
  cloud_init = templatefile("${path.module}/runner-user-data.tftpl", {
    redpanda_brokers = module.redpanda.broker_endpoints
  })
}

resource "aws_instance" "runner" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.runner_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.runner.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 100
    throughput  = 500
    iops        = 4000
  }

  tags = { Name = "${local.name_prefix}-runner" }
}

resource "aws_instance" "load_gen" {
  ami                    = data.aws_ssm_parameter.al2023_arm64_ami.value
  instance_type          = var.load_gen_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.load_gen.id]
  iam_instance_profile   = aws_iam_instance_profile.bench_host.name
  user_data              = local.cloud_init

  root_block_device {
    volume_type = "gp3"
    volume_size = 40
  }

  tags = { Name = "${local.name_prefix}-load-gen" }
}
```

The new `redpanda_brokers` template var is unused in this task — it's plumbed through so the next task only edits the template file, not `runner.tf`.

- [ ] **Step 3: Validate**

Run:
```bash
cd benchmarking/aws/terraform/shared
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 4: Confirm rendered output is byte-identical to the old inline form**

Run:
```bash
cd benchmarking/aws/terraform/shared
terraform console <<'EOF'
templatefile("./runner-user-data.tftpl", { redpanda_brokers = "10.42.10.10:9092,10.42.11.10:9092,10.42.10.11:9092" })
EOF
```

Expected: prints the rendered cloud-config — same content as the previous `local.cloud_init` heredoc, no additions yet.

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/terraform/shared/runner-user-data.tftpl benchmarking/aws/terraform/shared/runner.tf
git commit -m "refactor(bench/aws): extract runner cloud-init to templatefile (no behavior change)"
```

---

### Task 8: Install Kafka Connect on the runner via cloud-init

**Files:**
- Modify: `benchmarking/aws/terraform/shared/runner-user-data.tftpl`

- [ ] **Step 1: Append the KC install block**

Replace the entire `runner-user-data.tftpl` with:

```yaml
#cloud-config
package_update: true
packages:
  - postgresql15
  - mariadb1011
  - jq
  - unzip
  - tar
  - java-21-amazon-corretto-headless
write_files:
  - path: /opt/bench/.gitkeep
    content: ""
  - path: /opt/kafka-connect/worker.properties
    permissions: '0644'
    content: |
      bootstrap.servers=${redpanda_brokers}
      group.id=kc-bench-workers
      key.converter=org.apache.kafka.connect.json.JsonConverter
      value.converter=org.apache.kafka.connect.json.JsonConverter
      key.converter.schemas.enable=false
      value.converter.schemas.enable=false
      offset.storage.topic=_kc_offsets
      config.storage.topic=_kc_configs
      status.storage.topic=_kc_status
      offset.storage.replication.factor=3
      config.storage.replication.factor=3
      status.storage.replication.factor=3
      plugin.path=/opt/kafka-connect/plugins
      rest.port=8083
  - path: /etc/systemd/system/kafka-connect.service
    permissions: '0644'
    content: |
      [Unit]
      Description=Kafka Connect (single-worker distributed)
      After=network-online.target
      Wants=network-online.target

      [Service]
      Type=simple
      Environment=KAFKA_HEAP_OPTS=-Xmx2G
      ExecStart=/opt/kafka/bin/connect-distributed.sh /opt/kafka-connect/worker.properties
      Restart=on-failure
      User=root

      [Install]
      WantedBy=multi-user.target
runcmd:
  - mkdir -p /opt/bench
  - chmod 0755 /opt/bench
  - mkdir -p /opt/kafka /opt/kafka-connect/plugins

  # Apache Kafka tarball (we only need its libs + connect-distributed.sh).
  - curl -sSL https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz | tar -xz --strip-components=1 -C /opt/kafka

  # Debezium connectors.
  - curl -sSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.7.3.Final/debezium-connector-postgres-2.7.3.Final-plugin.tar.gz | tar -xz -C /opt/kafka-connect/plugins/
  - curl -sSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/2.7.3.Final/debezium-connector-mysql-2.7.3.Final-plugin.tar.gz | tar -xz -C /opt/kafka-connect/plugins/

  # Aiven JDBC sink + JDBC drivers (postgres + mariadb). Repo was renamed in 2024;
  # archive unpacks to jdbc-connector-for-apache-kafka-6.10.0/, not the old aiven-* name.
  - curl -sSL -o /tmp/aiven-jdbc.zip https://github.com/Aiven-Open/jdbc-connector-for-apache-kafka/releases/download/v6.10.0/jdbc-connector-for-apache-kafka-6.10.0.zip
  - unzip -q /tmp/aiven-jdbc.zip -d /opt/kafka-connect/plugins/
  - curl -sSL -o /opt/kafka-connect/plugins/jdbc-connector-for-apache-kafka-6.10.0/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
  - curl -sSL -o /opt/kafka-connect/plugins/jdbc-connector-for-apache-kafka-6.10.0/mariadb.jar https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.4.1/mariadb-java-client-3.4.1.jar
  - rm -f /tmp/aiven-jdbc.zip

  # Enable + start the worker. systemd retries on failure if Redpanda isn't yet
  # reachable (which is fine — the bench start waits for the runner anyway).
  - systemctl daemon-reload
  - systemctl enable --now kafka-connect.service
```

(Confluent S3 sink omitted from this plan — it has license-driven distribution constraints; we'll add it in Plan 4 alongside the first S3 sink scenario, which is the only place that needs it.)

- [ ] **Step 2: Validate**

Run:
```bash
cd benchmarking/aws/terraform/shared
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Sanity-check template renders**

Run:
```bash
cd benchmarking/aws/terraform/shared
terraform console <<'EOF'
templatefile("./runner-user-data.tftpl", { redpanda_brokers = "10.42.10.10:9092,10.42.11.10:9092,10.42.10.11:9092" })
EOF
```

Expected: rendered YAML containing `bootstrap.servers=10.42.10.10:9092,10.42.11.10:9092,10.42.10.11:9092` and the full KC install runcmd.

- [ ] **Step 4: Commit**

```bash
git add benchmarking/aws/terraform/shared/runner-user-data.tftpl
git commit -m "feat(bench/aws): install Kafka Connect + Debezium + Iceberg + Aiven JDBC on runner"
```

---

### Task 9: Plumb Redpanda outputs through main.go into MatrixRunner

**Files:**
- Modify: `benchmarking/aws/runner/matrix.go`
- Modify: `benchmarking/aws/runner/main.go`

- [ ] **Step 1: Add `RedpandaMetricsEndpoint` to `MatrixRunner` and `benchScriptArgs`**

Edit `benchmarking/aws/runner/matrix.go`, modify the existing `MatrixRunner` struct definition (around line 23) by adding the new field at the end:

```go
type MatrixRunner struct {
	SSM             SSMExecutor
	LogFetcher      LogFetcher
	RunnerInstance  string
	LoadGenInstance string
	ConfigPath      string
	BinaryPath      string
	Bucket          string
	SessionID       string
	// RedpandaMetricsEndpoint is the host:port pair (e.g. "10.42.10.10:9644") the
	// per-point scraper curls every 10s. Empty disables the scraper (Plan 1
	// safety net for environments without Redpanda yet).
	RedpandaMetricsEndpoint string
}
```

Modify `benchScriptArgs` (around line 184) similarly:

```go
type benchScriptArgs struct {
	VCPU                    int
	MemLimitGiB             int
	WarmupSec               int
	DurationSec             int
	ConfigPath              string
	BinaryPath              string
	Bucket                  string
	SessionID               string
	RedpandaMetricsEndpoint string
}
```

Modify the call site inside `MatrixRunner.Run` (around line 74) to pass it through:

```go
script := renderBenchScript(benchScriptArgs{
    VCPU:                    n,
    MemLimitGiB:             memLimitPerVCPU * n,
    WarmupSec:               int(warmup.Seconds()),
    DurationSec:             int(duration.Seconds()),
    ConfigPath:              m.ConfigPath,
    BinaryPath:              m.BinaryPath,
    Bucket:                  m.Bucket,
    SessionID:               m.SessionID,
    RedpandaMetricsEndpoint: m.RedpandaMetricsEndpoint,
})
```

- [ ] **Step 2: Wire the TF output through `runBench`**

In `benchmarking/aws/runner/main.go` at line ~220, the `MatrixRunner` struct literal is constructed and assigned to `mr`. The terraform outputs map is named `sharedOuts`. Add the new field:

```go
mr := &MatrixRunner{
    SSM:                     ssmExec,
    LogFetcher:              logFetcher,
    RunnerInstance:          sharedOuts["runner_instance_id"],
    LoadGenInstance:         sharedOuts["load_gen_instance_id"],
    ConfigPath:              "/opt/bench/config.yaml",
    BinaryPath:              "/opt/bench/redpanda-connect",
    Bucket:                  sharedOuts["results_bucket"],
    SessionID:               sessionID,
    RedpandaMetricsEndpoint: sharedOuts["redpanda_metrics_endpoint"],
}
```

(If `sharedOuts` does not contain `redpanda_metrics_endpoint` — e.g. terraform wasn't applied with the changes from tasks 1-6 — `sharedOuts["redpanda_metrics_endpoint"]` returns the zero value `""`, which the scraper subshell correctly treats as "no scraper.")

- [ ] **Step 3: Build to verify no compile errors**

Run:
```bash
go build ./benchmarking/aws/runner/...
```

Expected: success, no output.

- [ ] **Step 4: Run existing tests to verify no regression**

Run:
```bash
go test ./benchmarking/aws/runner/...
```

Expected: all tests pass. (No new behavior yet — only a new optional field with zero-value semantics.)

- [ ] **Step 5: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/main.go
git commit -m "feat(bench/aws): thread RedpandaMetricsEndpoint into MatrixRunner + benchScriptArgs"
```

---

### Task 10: Render the broker scraper subshell (TDD)

**Files:**
- Modify: `benchmarking/aws/runner/matrix_test.go`
- Modify: `benchmarking/aws/runner/matrix.go`

- [ ] **Step 1: Write the failing tests**

Append to `benchmarking/aws/runner/matrix_test.go` (don't touch existing tests):

```go
func TestRenderBenchScript_RedpandaScraperWhenEndpointSet(t *testing.T) {
	out := renderBenchScript(benchScriptArgs{
		VCPU:                    4,
		MemLimitGiB:             4,
		WarmupSec:               60,
		DurationSec:             900,
		ConfigPath:              "/tmp/cfg.yaml",
		BinaryPath:              "/opt/bench/rpcn",
		Bucket:                  "results-bucket",
		SessionID:               "sess-abc",
		RedpandaMetricsEndpoint: "10.42.10.10:9644",
	})
	if !strings.Contains(out, "RP=/tmp/redpanda-4.txt") {
		t.Errorf("expected RP path line for vcpu 4; got:\n%s", out)
	}
	if !strings.Contains(out, "curl -s --max-time 5 http://10.42.10.10:9644/public_metrics") {
		t.Errorf("expected redpanda scraper curl; got:\n%s", out)
	}
	if !strings.Contains(out, "RP_SCRAPER=$!") {
		t.Errorf("expected RP_SCRAPER pid capture; got:\n%s", out)
	}
	if !strings.Contains(out, `kill "$RP_SCRAPER" 2>/dev/null || true`) {
		t.Errorf("expected RP_SCRAPER kill on shutdown; got:\n%s", out)
	}
	if !strings.Contains(out, `aws s3 cp "$RP" "s3://results-bucket/runs/sess-abc/redpanda-4.txt"`) {
		t.Errorf("expected redpanda upload; got:\n%s", out)
	}
}

func TestRenderBenchScript_RedpandaScraperOmittedWhenEmpty(t *testing.T) {
	out := renderBenchScript(benchScriptArgs{
		VCPU:                    1,
		MemLimitGiB:             1,
		WarmupSec:               60,
		DurationSec:             900,
		ConfigPath:              "/tmp/cfg.yaml",
		BinaryPath:              "/opt/bench/rpcn",
		Bucket:                  "results-bucket",
		SessionID:               "sess-abc",
		RedpandaMetricsEndpoint: "",
	})
	if strings.Contains(out, "/public_metrics") {
		t.Errorf("expected no redpanda scraper when endpoint is empty; got:\n%s", out)
	}
	if strings.Contains(out, "redpanda-1.txt") {
		t.Errorf("expected no redpanda upload when endpoint is empty; got:\n%s", out)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
go test ./benchmarking/aws/runner -run TestRenderBenchScript_Redpanda -v
```

Expected: both tests FAIL with messages about missing content (e.g. "expected RP path line for vcpu 4").

- [ ] **Step 3: Implement the scraper subshell in `renderBenchScript`**

Edit `renderBenchScript` in `benchmarking/aws/runner/matrix.go`. Add the new subshell block and its kill/upload lines, gated on a non-empty endpoint.

Locate the existing prom scraper subshell (lines ~236-244, ending with `PROM_SCRAPER=$!`). Replace the section from `PROM_SCRAPER=$!` through the final `echo "log uploaded"` with:

```go
		`PROM_SCRAPER=$!`,
	}
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines,
			fmt.Sprintf(`RP=/tmp/redpanda-%d.txt`, a.VCPU),
			`: > "$RP"`,
			fmt.Sprintf(`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%%s)"
      curl -s --max-time 5 http://%s/public_metrics || echo "###scrape_error"
    } >> "$RP"
    sleep 10
  done
) &`, a.RedpandaMetricsEndpoint),
			`RP_SCRAPER=$!`,
		)
	}
	lines = append(lines,
		fmt.Sprintf(`sleep %d`, totalSec),
		`kill -TERM "$PID" 2>/dev/null || true`,
		`wait "$PID" 2>/dev/null || true`,
		`kill "$HEARTBEAT" 2>/dev/null || true`,
		`kill "$PROM_SCRAPER" 2>/dev/null || true`,
	)
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines, `kill "$RP_SCRAPER" 2>/dev/null || true`)
	}
	lines = append(lines,
		`echo "bench point complete"`,
		fmt.Sprintf(`aws s3 cp "$LOG" "s3://%s/runs/%s/sweep-%d.log" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
		fmt.Sprintf(`aws s3 cp "$PROM" "s3://%s/runs/%s/prom-%d.txt" >/dev/null`,
			a.Bucket, a.SessionID, a.VCPU),
	)
	if a.RedpandaMetricsEndpoint != "" {
		lines = append(lines,
			fmt.Sprintf(`aws s3 cp "$RP" "s3://%s/runs/%s/redpanda-%d.txt" >/dev/null`,
				a.Bucket, a.SessionID, a.VCPU),
		)
	}
	lines = append(lines, `echo "log uploaded"`)
	return strings.Join(lines, "\n")
}
```

This requires changing the function structure from `return strings.Join([]string{ ... }, "\n")` to a `lines := []string{ ... }` builder pattern. Update the start of `renderBenchScript` accordingly:

```go
func renderBenchScript(a benchScriptArgs) string {
	cpusetHi := 1 + a.VCPU
	totalSec := a.WarmupSec + a.DurationSec
	lines := []string{
		`set -euo pipefail`,
		fmt.Sprintf(`echo "starting bench: %d vCPU, %d GiB, warmup %ds, window %ds"`,
			a.VCPU, a.MemLimitGiB, a.WarmupSec, a.DurationSec),
		fmt.Sprintf(`LOG=/tmp/bench-%d.log`, a.VCPU),
		fmt.Sprintf(`PROM=/tmp/prom-%d.txt`, a.VCPU),
		`: > "$LOG"`,
		`: > "$PROM"`,
		fmt.Sprintf(`taskset -c 2-%d chrt --fifo 50 env GOMAXPROCS=%d GOMEMLIMIT=%dGiB REDPANDA_LICENSE_FILEPATH=/opt/bench/license.jwt %s run %s >"$LOG" 2>&1 &`,
			cpusetHi, a.VCPU, a.MemLimitGiB, a.BinaryPath, a.ConfigPath),
		`PID=$!`,
		`(
  while kill -0 "$PID" 2>/dev/null; do
    sleep 60
    LATEST="$(grep -F 'rolling stats' "$LOG" 2>/dev/null | tail -n 1 || true)"
    if [ -n "$LATEST" ]; then
      echo "[heartbeat] $LATEST"
    else
      echo "[heartbeat] connect running, no samples yet"
    fi
  done
) &`,
		`HEARTBEAT=$!`,
		`(
  while kill -0 "$PID" 2>/dev/null; do
    {
      echo "###timestamp=$(date +%s)"
      curl -s --max-time 5 http://localhost:4195/metrics || echo "###scrape_error"
    } >> "$PROM"
    sleep 10
  done
) &`,
		`PROM_SCRAPER=$!`,
	}
	// (then the appendage block from above)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
go test ./benchmarking/aws/runner -run TestRenderBenchScript -v
```

Expected: both new tests PASS. All previously-passing tests continue to pass.

- [ ] **Step 5: Run the full runner test suite to confirm no other tests broke**

Run:
```bash
go test ./benchmarking/aws/runner/...
```

Expected: PASS for every test.

- [ ] **Step 6: Commit**

```bash
git add benchmarking/aws/runner/matrix.go benchmarking/aws/runner/matrix_test.go
git commit -m "feat(bench/aws): broker-side scraper subshell uploads /public_metrics to S3"
```

---

### Task 11: Document the Plan 1 deliverables in the bench README

**Files:**
- Modify: `benchmarking/aws/README.md`

- [ ] **Step 1: Append a section to the README**

Add to `benchmarking/aws/README.md` (at the end of the file, before any trailing footer):

```markdown
## Kafka Connect comparison infra (Plan 1, 2026-05-22)

The shared stack now provisions a 3-broker Redpanda cluster (`im4gn.2xlarge`, NVMe instance store, static IPs in the private subnets) reachable from both `runner` and `load-gen` security groups. Topic/data paths in /var/lib/redpanda/data; admin + Prometheus on `:9644`; Kafka API on `:9092`. The runner EC2 cloud-init now also installs OpenJDK 21, the Apache Kafka 3.8 tarball, Debezium 2.7.3 (Postgres + MySQL), and the Aiven JDBC sink 6.10.0 (Iceberg + Confluent S3 sinks land in Plan 4) to `/opt/kafka-connect/plugins/`. A `kafka-connect.service` systemd unit runs the worker in single-worker distributed mode bound to `bootstrap.servers=<redpanda brokers>` on `:8083`.

For each sweep point, the bench script scrapes `http://<broker0>:9644/public_metrics` every 10s and uploads the framed dump to `s3://<results>/runs/<session>/redpanda-<vcpu>.txt` alongside the existing `prom-<vcpu>.txt`. **No bench numbers change in Plan 1** — Connect's pipeline still writes to `drop`. The broker scrape is captured for Plans 2/3 to use.

Acceptance smoke test (one-time, manual; see Plan 1 task 12):

```
# After `task aws:bench --validate-only` succeeds:
aws ssm start-session --target <runner-id>
$ curl -s localhost:8083/connector-plugins | jq '.[] | .class' | sort -u
$ curl -s http://<broker0>:9644/public_metrics | head -5
```
```

- [ ] **Step 2: Commit**

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): document Plan 1 infra (Redpanda cluster + KC install)"
```

---

### Task 12: AWS smoke test (operator-driven; one-time, manual)

**Files:** (no code changes; documentation of the verification procedure)

This task is run by the operator against a real AWS account to confirm the Plan 1 acceptance criteria from the spec.

- [ ] **Step 1: Build the runner binary**

Run:
```bash
cd /Users/prakhar.garg/Documents/connect_prakhar
aws-vault exec rp-bench -- task -d benchmarking/aws aws:validate
```

Expected: prints `validate: OK` after a `terraform plan` succeeds.

- [ ] **Step 2: Run a small smoke bench (1 vCPU point, 2-minute window)**

Create `/tmp/plan1-smoke.yaml`:

```yaml
name: postgres-orders-cdc-smoke
description: |
  Plan 1 smoke test — 1 vCPU, 2-min window. Confirms Connect runs and the
  new redpanda scraper uploads a non-empty file to S3.
connector: postgres_cdc
stack: postgres
infra:
  source:
    instance_class: db.r6g.large
    storage_gb: 50
    iops: 3000
    parameters:
      rds.logical_replication: "1"
  runner:
    instance_type: c8g.xlarge
dataset:
  initial_rows: 0
  row_size_bytes: 1200
  tables: [orders]
  seeder: cdc-rows
workload:
  write_rate_per_sec: 5000
  duration: 2m
  warmup: 30s
pipeline:
  input:
    postgres_cdc:
      dsn: ${POSTGRES_DSN}
      tls:
        skip_cert_verify: true
      stream_snapshot: false
      schema: public
      tables: [orders]
      slot_name: bench_slot
      batching:
        count: 5000
        period: 1s
matrix:
  cpu_points: [1]
reset:
  - sql: "SELECT pg_drop_replication_slot('bench_slot') FROM pg_replication_slots WHERE slot_name='bench_slot'"
  - sql: "TRUNCATE TABLE orders"
```

Run:
```bash
aws-vault exec rp-bench -- task -d benchmarking/aws aws:bench SCENARIO=/tmp/plan1-smoke.yaml
```

Expected: bench completes successfully; result JSON and markdown row produced; throughput numbers consistent with previous Plan-1-equivalent runs (sanity: ~5–15 MB/s at 5K msg/s, 1 vCPU on the smoke RDS class).

- [ ] **Step 3: Verify KC plugins are loaded on the runner**

While `aws:bench` is running (or with a follow-up `--keep` if implemented; otherwise during the next iteration), SSM into the runner:

```bash
aws ssm start-session --target $(terraform -chdir=benchmarking/aws/terraform/shared output -raw runner_instance_id) --profile rp-bench
$ sudo systemctl status kafka-connect    # should be active (running)
$ curl -s localhost:8083/connector-plugins | jq -r '.[].class' | sort -u
```

Expected output should contain:
```
io.debezium.connector.mysql.MySqlConnector
io.debezium.connector.postgresql.PostgresConnector
io.aiven.connect.jdbc.JdbcSinkConnector
```

- [ ] **Step 4: Verify Redpanda metrics endpoint is reachable**

From the runner SSM session:

```bash
$ BROKERS=$(curl -s http://169.254.169.254/latest/user-data | grep bootstrap.servers || true)
$ curl -s http://10.42.10.10:9644/public_metrics | head -10
```

Expected: ≥10 lines of Prometheus text-format metrics, with `# HELP` and `# TYPE` directives present.

- [ ] **Step 5: Verify Redpanda scrape was uploaded to S3**

```bash
aws-vault exec rp-bench -- aws s3 ls s3://<results-bucket>/runs/<session-id>/
```

Expected: the listing includes `redpanda-1.txt` alongside `sweep-1.log` and `prom-1.txt`. The `redpanda-1.txt` is non-empty (size > 0 bytes).

- [ ] **Step 6: Tear down**

```bash
aws-vault exec rp-bench -- task -d benchmarking/aws aws:down
```

Expected: all infra destroyed cleanly. Orphan-cleanup Lambda confirms no leftover resources within ~15 minutes (`SNS topic shows no message`).

- [ ] **Step 7: Record the smoke test result**

If steps 2-6 pass, commit a brief note to `benchmarking/aws/README.md` under the "Plan 1 (2026-05-22)" section with the actual session ID and a one-line summary of what worked.

```bash
git add benchmarking/aws/README.md
git commit -m "docs(bench/aws): record Plan 1 smoke test session"
```

If steps 2-6 FAIL: do NOT proceed to Plan 2. Fix the failure root cause (likely candidates: Redpanda package URL changed, JDBC mirror unreachable, AL2023 dnf metadata not yet refreshed at first boot — apply `dnf makecache` in user-data if so), update the relevant task above, and re-run.

---

## Verification checklist (Plan 1 acceptance)

- [ ] `terraform validate` passes for `terraform/modules/redpanda/`
- [ ] `terraform validate` passes for `terraform/shared/`
- [ ] `go test ./benchmarking/aws/runner/...` is green
- [ ] `go build ./benchmarking/aws/runner/...` succeeds
- [ ] Smoke bench (task 12) produces a result row in `docs/benchmark-results/postgres.md` consistent with pre-Plan-1 numbers
- [ ] `curl localhost:8083/connector-plugins` on the runner lists Debezium PG, Debezium MySQL, Aiven JDBC sink
- [ ] `curl http://<broker0>:9644/public_metrics` on the runner returns Prometheus text-format
- [ ] `s3://<results>/runs/<sess>/redpanda-1.txt` exists and is non-empty after smoke run
