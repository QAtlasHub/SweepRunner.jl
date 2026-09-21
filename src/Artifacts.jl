# Artifacts — scheduling around `DataVault.artifact!`.
#
# An artifact is built inside `work_fn` by whichever worker first needs it, and reused by the
# rest (DataVault). What the runtime adds is WHERE keys go: keys that share an artifact are
# better on one worker (it loads once, and no second worker waits on the build), and keys that
# need different ones are better spread (the builds run in parallel).

using ParamIO: artifact_identity

"""
    artifact_affinity(vault, name) -> Function

An `affinity` for [`run!`](@ref) that groups keys by the artifact `name` they need — its
`ParamIO.artifact_identity` — so a worker keeps drawing keys that reuse the artifact it just
built or loaded, and distinct artifacts start on distinct workers.

```julia
SweepRunner.run!(work_fn, vault, keys; affinity = artifact_affinity(vault, :ground_state))
```

Pair with `DataVault.artifact!(...; wait=false)` inside `work_fn` when keys outnumber workers:
a key whose artifact is mid-build then throws `DataVault.ArtifactBusy`, `run!` defers it without
spending an attempt, and the worker takes another key instead of blocking.
"""
function artifact_affinity(vault::Vault, name)
    spec = vault.spec
    n = string(name)
    return k -> artifact_identity(spec, n, k)
end

export artifact_affinity
