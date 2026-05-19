# Neo4j 5 + Qdrant — Local Graph Stack

## Prerequisites
- Docker Desktop (Mac/Windows) or Docker Engine + Compose plugin (Linux)

## Start the Stack

```bash
# From the repo root
docker compose -f docker/docker-compose.yml up -d

# Verify both services are healthy
docker compose -f docker/docker-compose.yml ps
```

## Access

| Service | URL | Credentials |
|---|---|---|
| Neo4j Browser | http://localhost:7474 | neo4j / dknexus2026 |
| Neo4j Bolt | bolt://localhost:7687 | neo4j / dknexus2026 |
| Qdrant REST | http://localhost:6333 | none |

## Apply Ontology Constraints (run once after first start)

```bash
# Using cypher-shell inside the container
docker exec -it dk_neo4j cypher-shell -u neo4j -p dknexus2026 \
  --file /var/lib/neo4j/import/01_constraints.cypher

# Or copy the file first
docker cp graph/cypher/01_constraints.cypher dk_neo4j:/var/lib/neo4j/import/
docker exec -it dk_neo4j cypher-shell -u neo4j -p dknexus2026 \
  --file /var/lib/neo4j/import/01_constraints.cypher
```

## Run the ETL Pipeline (after Neo4j is healthy)

```bash
# With your own ChatGPT export
python3 etl/run_pipeline.py --input path/to/conversations.json

# With synthetic sample data
python3 etl/run_pipeline.py
```

## Environment Variables

```bash
export NEO4J_URI="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASS="dknexus2026"
```

## Stop the Stack

```bash
docker compose -f docker/docker-compose.yml down
```
