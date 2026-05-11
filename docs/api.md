# API Reference

## Top-level

```{eval-rst}
.. autofunction:: mareforma.open
.. autofunction:: mareforma.schema
```

## EpistemicGraph

The agent-native interface. Do not instantiate directly — use
{func}`mareforma.open`.

```{eval-rst}
.. autoclass:: mareforma._graph.EpistemicGraph
   :members:
   :member-order: bysource
```

## Exceptions

```{eval-rst}
.. autoexception:: mareforma.db.DatabaseError
.. autoexception:: mareforma.db.ClaimNotFoundError
```
