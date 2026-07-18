"""Public API export verification (FEAT-313 TASK-1825)."""
from navigator_eventbus import lifecycle

EXPECTED_EXPORTS = {
    "TraceContext", "LifecycleEvent", "SubscriberErrorEvent",
    "EventRegistry", "AsyncSubscriber", "get_global_registry", "scope",
    "EventProvider", "EventEmitterMixin", "set_bootstrap_hook",
    "wire_events", "register_event_names",
    "LoggingSubscriber", "WebhookSubscriber",
}

MUST_NOT_EXIST = {
    "BeforeInvokeEvent", "AfterInvokeEvent", "InvokeFailedEvent",
    "BeforeClientCallEvent", "AfterClientCallEvent", "ClientCallFailedEvent",
    "ClientStreamChunkEvent", "BeforeToolCallEvent", "AfterToolCallEvent",
    "ToolCallFailedEvent", "MessageAddedEvent", "AgentInitializedEvent",
    "AgentConfiguredEvent", "ToolManagerReadyEvent", "AgentStatusChangedEvent",
    "OpenTelemetrySubscriber", "FlowStartedEvent", "FlowCompletedEvent",
    "NodeStartedEvent", "NodeCompletedEvent", "NodeFailedEvent",
    "NodeSkippedEvent",
}


class TestPublicAPI:
    def test_expected_exports_present(self):
        actual = set(lifecycle.__all__)
        assert EXPECTED_EXPORTS.issubset(actual), f"Missing: {EXPECTED_EXPORTS - actual}"

    def test_typed_events_absent(self):
        actual = set(lifecycle.__all__)
        overlap = MUST_NOT_EXIST & actual
        assert not overlap, f"Typed events leaked into package: {overlap}"

    def test_all_exports_are_importable_attributes(self):
        for name in lifecycle.__all__:
            assert hasattr(lifecycle, name), f"{name} declared in __all__ but not importable"

    def test_top_level_package_exposes_lifecycle(self):
        import navigator_eventbus
        assert navigator_eventbus.lifecycle is lifecycle
        assert "lifecycle" in navigator_eventbus.__all__
