# CONTAINERIZED ARCHITECTURE FOR TURBOPREDICT X PROTEAN

## Problem Statement
Current monolithic architecture has all 13 units sharing the same codebase, causing:
- Code changes affecting all units simultaneously
- Lack of isolation between units
- Risk of system-wide failures
- Difficulty in unit-specific maintenance

## Proposed Architecture

### Container Strategy
Each unit gets its own dedicated container with:
- Independent codebase instance
- Isolated data processing
- Unit-specific configuration
- Individual scaling capability

### Architecture Components

#### 1. Unit Containers (13 containers)
```
turbopredict-k-12-01/     - Container for K-12-01 unit
turbopredict-k-16-01/     - Container for K-16-01 unit
turbopredict-k-19-01/     - Container for K-19-01 unit
turbopredict-k-31-01/     - Container for K-31-01 unit
turbopredict-xt-07002/    - Container for XT-07002 unit
turbopredict-c-02001/     - Container for C-02001 unit
turbopredict-c-104/       - Container for C-104 unit
turbopredict-c-13001/     - Container for C-13001 unit
turbopredict-c-1301/      - Container for C-1301 unit
turbopredict-c-1302/      - Container for C-1302 unit
turbopredict-c-201/       - Container for C-201 unit
turbopredict-c-202/       - Container for C-202 unit
turbopredict-07-mt01-k001/ - Container for 07-MT01-K001 unit
```

#### 2. Management Container
```
turbopredict-orchestrator/  - Central management and coordination
```

#### 3. Shared Services
```
turbopredict-db/           - Shared database services
turbopredict-api/          - API gateway for external access
turbopredict-monitor/      - System monitoring and health checks
```

### Data Isolation Strategy

#### Unit-Specific Data Volumes
```
/data/units/K-12-01/       - K-12-01 specific data
/data/units/K-16-01/       - K-16-01 specific data
/data/units/K-19-01/       - K-19-01 specific data
... (and so on for each unit)
```

#### Shared Data Volumes
```
/data/shared/excel/        - Excel templates and PI DataLink files
/data/shared/config/       - Global configuration files
/data/shared/logs/         - Centralized logging
```

### Container Communication

#### Internal Network
- Private Docker network for container-to-container communication
- Service discovery via container names
- Load balancing for API access

#### API Endpoints
Each unit container exposes:
- `/health` - Health check endpoint
- `/status` - Unit status and data freshness
- `/refresh` - Trigger manual refresh
- `/analyze` - Run analysis functions
- `/metrics` - Prometheus metrics

### Configuration Management

#### Unit-Specific Configuration
```yaml
# config/units/K-12-01.yml
unit_id: "K-12-01"
plant: "PCFS"
excel_file: "PCFS_Automation.xlsx"
refresh_interval: "1h"
memory_limit: "2GB"
cpu_limit: "1.0"
specific_tags:
  - "K12001.PV"
  - "K12001.SP"
```

#### Global Configuration
```yaml
# config/global.yml
database:
  type: "postgresql"
  host: "turbopredict-db"
  port: 5432
monitoring:
  prometheus_port: 9090
  grafana_port: 3000
logging:
  level: "INFO"
  centralized: true
```

### Deployment Strategy

#### Docker Compose Structure
```yaml
version: '3.8'
services:
  # Unit containers
  k-12-01:
    build: ./containers/unit-base
    environment:
      UNIT_ID: K-12-01
    volumes:
      - ./data/units/K-12-01:/app/data
      - ./config/units/K-12-01.yml:/app/config.yml

  k-16-01:
    build: ./containers/unit-base
    environment:
      UNIT_ID: K-16-01
    volumes:
      - ./data/units/K-16-01:/app/data
      - ./config/units/K-16-01.yml:/app/config.yml

  # ... (repeat for all units)

  # Management services
  orchestrator:
    build: ./containers/orchestrator
    ports:
      - "8080:8080"
    depends_on:
      - k-12-01
      - k-16-01
      # ... (all unit containers)

  # Monitoring
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
```

### Benefits

#### Isolation
- Each unit runs independently
- Code changes can be deployed per unit
- Failures in one unit don't affect others

#### Scalability
- Scale individual units based on load
- Resource allocation per unit
- Independent update cycles

#### Maintainability
- Unit-specific debugging
- Isolated testing environments
- Easier troubleshooting

#### Reliability
- Fault isolation
- Independent restart capability
- Progressive deployment strategies

### Migration Strategy

#### Phase 1: Containerize Existing Code
1. Create base Docker image with current codebase
2. Generate unit-specific configurations
3. Set up data volume mapping

#### Phase 2: Implement Orchestration
1. Create orchestrator container
2. Set up inter-container communication
3. Implement health checks

#### Phase 3: Add Monitoring
1. Set up Prometheus metrics
2. Configure Grafana dashboards
3. Implement alerting

#### Phase 4: Production Deployment
1. Test in staging environment
2. Progressive rollout by unit
3. Monitor and validate

### File Structure
```
CodeX/
├── containers/
│   ├── unit-base/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── entrypoint.sh
│   ├── orchestrator/
│   │   ├── Dockerfile
│   │   └── app.py
│   └── monitoring/
├── config/
│   ├── global.yml
│   └── units/
│       ├── K-12-01.yml
│       ├── K-16-01.yml
│       └── ... (all units)
├── data/
│   ├── shared/
│   └── units/
│       ├── K-12-01/
│       ├── K-16-01/
│       └── ... (all units)
├── docker-compose.yml
└── docker-compose.override.yml
```