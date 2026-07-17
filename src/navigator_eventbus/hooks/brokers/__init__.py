"""Broker hooks (redis, rabbitmq, sqs, mqtt) — lazy-import `navigator.brokers`.

Phase 1 (FEAT-312) keeps these lazy-importing `navigator.brokers`; Phase 3
(`eventbus-brokers-port`) recables them to the internal transport layer.
Populated by TASK-1803 (FEAT-312, Module 6).
"""
