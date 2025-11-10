# 🚀 AI Agents Quick Reference Card

**One-page reference for TURBOPREDICT AI Agents**

---

## 30-Second Setup

```python
from pi_monitor.agents import get_agent_manager

# Initialize once
agent_manager = get_agent_manager(enable_agents=True)
```

---

## Common Operations

### 1️⃣ Predict Timeout
```python
timeout = agent_manager.predict_timeout(tag, plant)
# Returns: optimal timeout in seconds
```

### 2️⃣ Filter Dead Tags
```python
filtered_tags, skipped = agent_manager.filter_tags(tags, unit)
# Returns: (active_tags, skipped_info)
```

### 3️⃣ Calculate Batch Size
```python
batch_size = agent_manager.calculate_batch_size(unit, records, columns)
# Returns: optimal batch size (rows)
```

### 4️⃣ Learn from Fetch
```python
agent_manager.learn_from_fetch(tag, plant, timeout, success, elapsed_time)
# No return - updates agents
```

### 5️⃣ Learn from Batch
```python
agent_manager.record_batch_performance(
    unit, batch_size, time, memory_gb, success, records, columns
)
# No return - updates agents
```

---

## Integration Templates

### Template A: Basic Fetch Loop
```python
from pi_monitor.agents import get_agent_manager
import time

agent_manager = get_agent_manager(enable_agents=True)

for tag in tags:
    # 1. Check if dead
    should_skip, reason = agent_manager.should_skip_tag(tag)
    if should_skip:
        print(f"Skip {tag}: {reason}")
        continue

    # 2. Predict timeout
    timeout = agent_manager.predict_timeout(tag, plant)

    # 3. Fetch
    start = time.time()
    try:
        result = fetch_pi_data(tag, timeout)
        success = True
    except TimeoutError:
        success = False

    # 4. Learn
    agent_manager.learn_from_fetch(
        tag, plant, timeout, success, time.time() - start
    )
```

### Template B: Batch Processing
```python
from pi_monitor.agents import get_agent_manager
import time

agent_manager = get_agent_manager(enable_agents=True)

# Calculate optimal batch
batch_size = agent_manager.calculate_batch_size(
    unit, len(df), len(df.columns)
)

# Process
start = time.time()
try:
    for i in range(0, len(df), batch_size):
        process_chunk(df.iloc[i:i+batch_size])
    success = True
except MemoryError:
    success = False

# Record
agent_manager.record_batch_performance(
    unit, batch_size, time.time() - start,
    get_memory_gb(), success, len(df), len(df.columns)
)
```

### Template C: Using Context Managers
```python
from pi_monitor.agents import get_agent_manager, FetchTimer, BatchTimer

agent_manager = get_agent_manager(enable_agents=True)

# Auto-learn fetch
with FetchTimer(agent_manager, tag, plant, timeout):
    result = fetch_pi_data(tag)

# Auto-learn batch
with BatchTimer(agent_manager, unit, batch_size, records, columns):
    process_in_batches(df, batch_size)
```

---

## Monitoring

### Quick Status
```python
agent_manager.print_all_summaries()
```

### Get Metrics
```python
metrics = agent_manager.get_combined_metrics()
print(f"Dead tags: {metrics['tag_health']['dead_tags']}")
print(f"Time saved: {metrics['timeout_predictor']['total_time_saved']}")
```

### Save State
```python
agent_manager.save_all_agents()
# Auto-saves every 10 operations anyway
```

---

## Configuration

**File:** `config/agents.json`

### Enable/Disable Agent
```json
{
  "agents": {
    "timeout_predictor": {
      "enabled": true,    ← Change to false to disable
      "config": { ... }
    }
  }
}
```

### Adjust Thresholds
```json
{
  "agents": {
    "tag_health": {
      "config": {
        "dead_threshold": 0.1,     ← <10% success = dead
        "sick_threshold": 0.5,     ← <50% success = sick
        "min_attempts": 5          ← Need 5+ samples
      }
    }
  }
}
```

### Adjust Timeouts
```json
{
  "agents": {
    "timeout_predictor": {
      "config": {
        "min_timeout": 10.0,       ← Minimum 10s
        "max_timeout": 600.0,      ← Maximum 600s
        "safety_margin": 1.2,      ← Add 20% safety
        "plant_base_timeouts": {
          "ABF": 600.0,
          "PCMSB": 360.0,
          "PCFS": 60.0
        }
      }
    }
  }
}
```

---

## Troubleshooting

### ❌ Agents Not Learning
```python
# Check that learning methods are called
agent_manager.learn_from_fetch(...)  # After each fetch
agent_manager.record_batch_performance(...)  # After each batch

# Verify sufficient data (need 5-10 samples)
metrics = agent_manager.get_combined_metrics()
print(metrics)
```

### ❌ Import Errors
```bash
# Test import
python -c "from pi_monitor.agents import get_agent_manager; print('OK')"

# Check file exists
ls pi_monitor/agents/__init__.py
```

### ❌ Tags Incorrectly Marked Dead
```json
// In config/agents.json, increase thresholds:
{
  "agents": {
    "tag_health": {
      "config": {
        "dead_threshold": 0.2,  // Was 0.1, now allow 20% failure
        "min_attempts": 10      // Was 5, now need more samples
      }
    }
  }
}
```

### ❌ Memory Errors Persist
```json
// In config/agents.json, reduce batch sizes:
{
  "agents": {
    "batch_optimizer": {
      "config": {
        "max_batch": 500000,  // Was 1000000, now smaller
        "default_batch": 100000  // Was 250000, now smaller
      }
    }
  }
}
```

---

## Expected Results

| Metric | Improvement | Timeline |
|--------|-------------|----------|
| Refresh cycle time | 40-50% faster | After 1-2 weeks |
| Dead tag fetches | 100% eliminated | Immediate |
| Memory errors | 50-80% reduction | After 1 week |
| Timeout waste | 40-60% reduction | After 1-2 weeks |

---

## File Locations

```
📁 Project Root
├── 📄 config/agents.json                      # Configuration
├── 📂 models/agents/                          # Saved agents
│   ├── timeout_predictor_v1.0.0.pkl
│   ├── tag_health_v1.0.0.pkl
│   └── batch_optimizer_v1.0.0.pkl
├── 📂 logs/agent_performance/                 # Logs
│   ├── timeout_predictor_YYYYMMDD.jsonl
│   └── ...
├── 📂 pi_monitor/agents/                      # Source code
│   ├── __init__.py
│   ├── base_agent.py
│   ├── timeout_predictor.py
│   ├── tag_health.py
│   ├── batch_optimizer.py
│   └── integration.py
└── 📂 docs/                                   # Documentation
    ├── AI_AGENTS_INTEGRATION_GUIDE.md
    └── AGENTS_QUICK_REFERENCE.md (this file)
```

---

## API Cheat Sheet

| Method | Purpose | Returns |
|--------|---------|---------|
| `predict_timeout(tag, plant)` | Get optimal timeout | `float` (seconds) |
| `should_skip_tag(tag)` | Check if tag is dead | `(bool, str)` |
| `filter_tags(tags, unit)` | Remove dead tags | `(list, list)` |
| `calculate_batch_size(unit, records, cols)` | Get optimal batch | `int` (rows) |
| `learn_from_fetch(...)` | Update after fetch | None |
| `record_batch_performance(...)` | Update after batch | None |
| `print_all_summaries()` | Show performance | None |
| `save_all_agents()` | Save state | None |
| `get_combined_metrics()` | Get all metrics | `dict` |

---

## Disable Agents

```python
# Method 1: At initialization
agent_manager = get_agent_manager(enable_agents=False)

# Method 2: In config
# Edit config/agents.json, set "enabled": false

# Method 3: Environment variable (if implemented)
# os.environ['DISABLE_AGENTS'] = '1'
```

---

## Further Reading

- 📖 [Full Integration Guide](AI_AGENTS_INTEGRATION_GUIDE.md)
- 📖 [Implementation Summary](../AI_AGENTS_IMPLEMENTATION.md)
- 💻 [Usage Examples](../examples/agent_usage_example.py)
- 🏗️ [Design Spec](../AI_AGENT_RECOMMENDATIONS.md)

---

**Need help?** Check `logs/agent_performance/` for detailed logs.

**Quick test:** `python examples/agent_usage_example.py`

🚀 **Happy optimizing!**
