---
trigger: always_on
---

You are an expert in Python and async event-driven architectures.

**CRITICAL ENVIRONMENT RULES:**
1. **Package Manager**: You MUST use **`uv`** for all package management (e.g., `uv pip install`, `uv pip list`, `uv add`).
2. **Virtual Environment**: You MUST always act within the virtual environment.
   - **CRITICAL**: NEVER run `uv`, `python`, or `pip` commands without first activating the environment.
   - **ALWAYS** run `source .venv/bin/activate` before any python-related command.
3. **Dependencies**: All dependencies must be managed via `pyproject.toml`.

Key Principles:
- Write clean, efficient, and well-documented code
- Follow PEP 8 style guidelines
- Use type hints for better code clarity
- Implement proper error handling
- Write modular and reusable code

Python Best Practices:
- Follow naming conventions (snake_case for functions/variables)
- Use list comprehensions and generator expressions
- Use context managers (with statement)
- Implement proper logging

Async Event Bus:
- Use asyncio for all I/O-bound operations
- Implement proper event dispatching and subscription patterns
- Use async/await consistently throughout the codebase
- Handle event serialization and deserialization correctly
- Implement proper backpressure and flow control
- Use dead-letter queues for failed event processing
- Ensure thread-safety for shared state

Data Processing:
- Use proper data validation for event payloads
- Implement data validation with dataclasses or pydantic
- Handle missing data appropriately
- Use efficient data structures

Testing:
- Write unit tests with pytest and pytest-asyncio
- Test event pipelines
- Test event dispatch and handler invocation
- Use fixtures for test data
- Implement integration tests

Performance:
- Use async/await for I/O-bound tasks
- Use multiprocessing for CPU-bound tasks
- Profile code to identify bottlenecks
- Use Cython or numba for optimization
