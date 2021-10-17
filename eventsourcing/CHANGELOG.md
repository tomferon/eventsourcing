# Revision history for eventsourcing

## 1.0.0 -- ??

* Add `InterruptibleStreamFamily` which wraps an underlying stream family into
  a new one which stops producing new event batches under some condition.
* `TopUpReadModel`: A read model which fetches a projected state from an
  underlying read model, looks for new events since the projection last ran and
  applies an aggregator.
* Add `ReadModelP` to turn any read model into a profunctor.

## 0.9.0 -- 2020-08-16

* First version.
