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

## BuildContext

The pipeline interface. Injected into every `@transform` function by the
runner.

```{eval-rst}
.. autoclass:: mareforma.pipeline.context.BuildContext
   :members: claim, save, load, source_path, log, params, run_id, root
   :member-order: bysource
```

## Exceptions

```{eval-rst}
.. autoexception:: mareforma.db.DatabaseError
.. autoexception:: mareforma.db.ClaimNotFoundError
.. autoexception:: mareforma.pipeline.context.ArtifactNotFoundError
.. autoexception:: mareforma.pipeline.context.ArtifactSaveError
```
