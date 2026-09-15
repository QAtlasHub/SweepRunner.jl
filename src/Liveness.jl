# Liveness — is the master that holds this lock still running?
#
# `stale_after` answers that by waiting. It has to, because a heartbeat can only ever say
# "recently alive"; the absence of one is indistinguishable from a slow filesystem until enough
# time has passed. Where the question CAN be asked outright, asking is worth several minutes of a
# batch allocation.
#
# Nothing here is required for correctness: every path degrades to `:unknown`, and `stale_after`
# then decides exactly as before. `:slurm` is one of four worker modes, so a mechanism that only
# worked there would cover a quarter of the runs.

using Dates

"""
    owner_token() -> String

This master's identity, stamped into `.running` by [`run!`](@ref) so a sibling can ask about it
later. `host:pid:nonce` from DataVault, with `:slurm<jobid>` appended inside a Slurm allocation.

The Slurm field is what makes the question answerable ACROSS hosts: `/proc` only works for a
holder on this machine, and the master of the job that was killed is usually somewhere else.
"""
function owner_token()::String
    base = DataVault.new_owner_token()
    job = get(ENV, "SLURM_JOB_ID", "")
    return isempty(job) ? base : string(base, ":slurm", job)
end

"""
    holder_liveness(owner) -> Symbol

`:alive`, `:dead`, or `:unknown` for the holder named by an [`owner_token`](@ref).

`:dead` is only ever returned on positive evidence that the process is gone. Everything else is
`:unknown`, including every error path: a wrong `:dead` would hand a live master's key to someone
else, which is the one outcome the lock exists to prevent.

Two sources, in order:

1. a Slurm job id, when this process is ITSELF inside a Slurm allocation. The job is absent from
   the queue, so it has finished, been cancelled, or hit its wall clock.
2. the pid, when the holder is on THIS host and `/proc` exists. A recycled pid reads as `:alive`,
   which is the safe direction.
"""
function holder_liveness(owner::AbstractString)::Symbol
    parts = split(String(owner), ':')
    length(parts) >= 3 || return :unknown

    job = findfirst(p -> startswith(p, "slurm"), parts)
    if job !== nothing
        s = _slurm_liveness(parts[job][6:end])
        s === :unknown || return s
    end

    parts[1] == gethostname() || return :unknown
    pid = tryparse(Int, parts[2])
    pid === nothing && return :unknown
    return _pid_liveness(pid)
end

# `/proc/<pid>` is the whole check on Linux. Elsewhere there is no equally cheap answer that does
# not risk a false `:dead`, so there is no answer.
function _pid_liveness(pid::Int)::Symbol
    Sys.islinux() || return :unknown
    return isdir("/proc/$(pid)") ? :alive : :dead
end

# The live-job set, cached: `squeue` is a scheduler RPC and this is consulted per contended key.
const _SQUEUE_TTL = 15.0
const _squeue_cache = Ref{Tuple{Float64,Union{Set{String},Nothing}}}((-Inf, nothing))
const _squeue_lock = ReentrantLock()

function _slurm_liveness(jobid::AbstractString)::Symbol
    isempty(jobid) && return :unknown
    # Ask Slurm only from INSIDE a Slurm allocation, so the queue being consulted is demonstrably
    # the one that would have run the holder. `squeue` existing proves nothing: measured on the
    # development box behind this package, `/home/…/.local/bin/squeue` is a wrapper that answers
    # about a REMOTE cluster's queue, and every job id from anywhere else reads as absent there.
    # Absent would then mean `:dead`, and a false `:dead` hands a live master's key to someone
    # else, which is the one outcome the lock exists to prevent.
    haskey(ENV, "SLURM_JOB_ID") || return :unknown
    live = _live_slurm_jobs()
    live === nothing && return :unknown
    # An array task is `12345_7` in the queue while `SLURM_JOB_ID` is `12345`, so the whole job is
    # alive if any of its tasks is.
    jobid in live && return :alive
    any(j -> startswith(j, jobid * "_"), live) && return :alive
    return :dead
end

# `nothing` means "no opinion": squeue is absent, or it failed. Listing the queue and testing
# membership is deliberate. `squeue -j <id>` exits non-zero BOTH for a finished job and for a
# broken scheduler, and those must not collapse into the same answer.
function _live_slurm_jobs()::Union{Set{String},Nothing}
    return lock(_squeue_lock) do
        t, cached = _squeue_cache[]
        time() - t < _SQUEUE_TTL && return cached
        fresh = try
            if Sys.which("squeue") === nothing
                nothing
            else
                out = read(pipeline(`squeue -h -o %i`; stderr=devnull), String)
                Set(String.(split(out; keepempty=false)))
            end
        catch
            nothing
        end
        _squeue_cache[] = (time(), fresh)
        return fresh
    end
end

export owner_token, holder_liveness
