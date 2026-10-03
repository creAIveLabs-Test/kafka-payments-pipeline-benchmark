# Running on Windows (WSL2)

Tested setup: Windows 10/11, Intel Core i7 (10th gen), 48 GB RAM, SSD. The GPU is not used; Kafka, Redis and PostgreSQL are CPU, memory and disk workloads.

The scripts are bash, so they run inside **WSL2 (Ubuntu)**, with **Docker Desktop** providing Docker to WSL.

## 1. Install WSL2 and Ubuntu

In PowerShell (as Administrator):

```powershell
wsl --install -d Ubuntu
```

Restart if asked, then open "Ubuntu" from the Start menu and create a Linux user.

## 2. Give WSL2 enough CPU and memory

Create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=32GB
processors=12
swap=8GB
```

`processors` should be the number of logical processors you want to give it (an i7-10th gen has 12 or 16; leave 2-4 for Windows). Then in PowerShell:

```powershell
wsl --shutdown
```

## 3. Install Docker Desktop

1. Install Docker Desktop for Windows.
2. Settings > General: enable **Use the WSL 2 based engine**.
3. Settings > Resources > WSL Integration: enable your **Ubuntu** distro.
4. In Ubuntu, check: `docker run --rm hello-world`

## 4. Install Java, Git and Python in Ubuntu

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk git python3
java -version
```

## 5. Get the project (inside the Linux filesystem)

Clone into your Linux home directory, **not** under `/mnt/c/...`. Files on the Windows drive are much slower from WSL and will distort the results.

```bash
cd ~
git clone https://github.com/creAIveLabs-Test/kafka-payments-pipeline-benchmark.git
cd kafka-payments-pipeline-benchmark
./mvnw -q package -DskipTests
```

## 6. Run

```bash
# single broker, the same experiments as the published laptop results
./scripts/experiments.sh

# 3-broker cluster: scaling, consumer crash, broker crash, scale-out, LinkedIn replication tests
./scripts/experiments-distributed.sh

# give each Kafka broker more memory on a big machine
export KAFKA_HEAP_OPTS="-Xms2g -Xmx2g"
```

Results go to `results/runs/<name>/RESULTS.md` and `results/SUMMARY.md`. The run records the CPU, core count and RAM that WSL sees, so results from different machines stay labeled.

## Suggested settings for a 48 GB / i7 machine

| Setting | Value | Why |
|---|---|---|
| `.wslconfig` memory | 32 GB | Room for 3 brokers, Redis, Postgres and up to 12 Java processes |
| `KAFKA_HEAP_OPTS` | `-Xms2g -Xmx2g` | Broker heap; the rest of the memory goes to the OS page cache, which Kafka relies on |
| `JAVA_OPTS` | `-Xms512m -Xmx2g` | Per stage process |
| `PARTITIONS` | 24 | Enough partitions for 4 instances x 4 threads |
| `N` | 2,000,000 or 10,000,000 | 10M gives longer steady state; needs about 10 GB of disk during a cluster run |

## Troubleshooting

- **`docker: command not found` in Ubuntu**: Docker Desktop WSL integration is off for this distro (step 3).
- **Ports 9092, 9094, 9096, 5432 or 6379 in use**: stop other Kafka, Postgres or Redis installs on Windows.
- **Very slow runs**: make sure the project is under `~/`, not `/mnt/c/`.
- **Out of memory**: raise `memory` in `.wslconfig`, or use less with `KAFKA_HEAP_OPTS="-Xms1g -Xmx1g"` and `INSTANCES=2`.
