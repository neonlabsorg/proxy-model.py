from aioprometheus import Counter, Registry
from aioprometheus.service import Service
from aioprometheus.mypy_types import LabelsType, NumericValueType


class PositiveCounter(Counter):
    def add(self, labels: LabelsType, amount: NumericValueType) -> None:
        try:
            current = self.get(labels)
        except KeyError:
            current = 0

        value = current + amount

        self.set(labels, 0 if value < 0 else value)
