"""
Circuit Breaker Pattern for Google Drive API calls.
Prevents cascading failures by halting calls when error threshold is exceeded.
"""
import time
import threading
import logging
import os
from enum import Enum

logger = logging.getLogger("CircuitBreaker")


class CircuitState(Enum):
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing — reject all calls
    HALF_OPEN = "half_open" # Testing recovery


class CircuitBreakerOpenError(Exception):
    """Raised when the circuit breaker is OPEN and rejecting calls."""
    pass


class CircuitBreaker:
    """
    Thread-safe circuit breaker.
    
    State transitions:
        CLOSED → (failures >= threshold) → OPEN
        OPEN → (recovery_timeout elapsed) → HALF_OPEN
        HALF_OPEN → (success) → CLOSED
        HALF_OPEN → (failure) → OPEN
    """

    def __init__(self, name="default", failure_threshold=None, recovery_timeout=None):
        self.name = name
        self.failure_threshold = int(
            failure_threshold or os.getenv('CIRCUIT_FAILURE_THRESHOLD', '5')
        )
        self.recovery_timeout = int(
            recovery_timeout or os.getenv('CIRCUIT_RECOVERY_TIMEOUT', '300')
        )
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self._lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """
        Execute func through the circuit breaker.
        Raises CircuitBreakerOpenError if circuit is OPEN.
        """
        with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    logger.info(f"[{self.name}] Circuit transitioning OPEN → HALF_OPEN")
                    self.state = CircuitState.HALF_OPEN
                else:
                    remaining = self.recovery_timeout - (time.time() - self.last_failure_time)
                    raise CircuitBreakerOpenError(
                        f"Circuit '{self.name}' is OPEN. Recovery in {remaining:.0f}s"
                    )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                logger.info(f"[{self.name}] Circuit recovered → CLOSED")
            self.state = CircuitState.CLOSED

    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                logger.warning(
                    f"[{self.name}] Failure threshold reached ({self.failure_count}/"
                    f"{self.failure_threshold}). Circuit → OPEN"
                )
                self.state = CircuitState.OPEN

    @property
    def is_open(self):
        return self.state == CircuitState.OPEN

    def reset(self):
        """Manual reset for admin/testing purposes."""
        with self._lock:
            self.failure_count = 0
            self.last_failure_time = None
            self.state = CircuitState.CLOSED
            logger.info(f"[{self.name}] Circuit manually reset → CLOSED")
