# Prerequisite — a stage that must finish before the stage that depends on it.

using DataVault
using ParamIO: DataKey

"""
    Prerequisite(work_fn, vault, keys; opts=nothing)

Work that a later stage's keys share. Its three fields are `run!`'s three arguments, because that
is what it becomes: its own key space, its own vault, its own payloads.

`opts` overrides the dependent stage's [`RunOpts`](@ref) for the prerequisite alone, which is
usually about `stale_after`: the shared setup is typically the slow half, and a lock reclaimed
mid-build is the thing this exists to prevent.

`stop_flag` and `deadline` are not stage knobs: they bound the JOB, and a stage may not loosen a
bound the job set. `deadline` therefore takes the TIGHTER of the two, and `stop_flag` takes the
caller's whenever it has one. A prerequisite cannot redirect or outlive either.

That asymmetry is deliberate for `stop_flag`: [`RunOpts`](@ref) resolves its default from
`ENV["SWEEPRUNNER_STOP_FLAG"]`, so an `opts` here that never mentions `stop_flag` can still carry
one, and `=== nothing` does not mean "the author left it unset" for that field.

Build `keys` by projecting the dependent key space onto the axes the setup actually depends on
(`ParamIO.project`), so the two spaces cannot drift apart by hand.
"""
struct Prerequisite
    work_fn::Function
    vault::DataVault.Vault
    keys::Vector{DataKey}
    opts::Union{RunOpts,Nothing}
end

function Prerequisite(
    work_fn::Function,
    vault::DataVault.Vault,
    keys::AbstractVector{DataKey};
    opts::Union{RunOpts,Nothing}=nothing,
)
    return Prerequisite(work_fn, vault, collect(keys), opts)
end

"""
    run_prerequisite!(p; opts=RunOpts(), load=nothing, poll=30.0) -> NamedTuple

Run `p` until EVERY one of its keys is done, and report whether that happened:

    (; complete, remaining, done, waited, rounds, stopped_by)

`complete` is the only field a caller has to read. The rest say why not: `remaining` keys are
undone, `waited` counts the rounds spent purely waiting for a sibling master.

This is a barrier, not a work loop, and the difference is what it does when it has nothing left to
take. [`run_loop!`](@ref) stops after `max_empty_rounds` empty rounds, which is right when the keys
are independent. Here the dependent stage cannot start until the setup exists, so a round that
finds every remaining key locked by a sibling SLEEPS and goes again.

It terminates on: every key done; no progress AND no key held by a sibling (a genuine failure);
`opts.stop_flag`; `opts.deadline`. A live sibling building a slow setup is waited for, which is the
point; a dead one is bounded by `stale_after`, after which its lock is reclaimable.
"""
function run_prerequisite!(
    p::Prerequisite; opts::RunOpts=RunOpts(), load=nothing, poll::Real=30.0
)
    o = _merged_opts(p, opts)
    n_done = 0
    waited = 0
    rounds = 0

    while true
        stopped = _stop_reason(o)
        if stopped !== nothing
            # A barrier whose keys are all done is satisfied whether or not a stop is pending;
            # `complete=false` beside `remaining=0` would gate the dependent stage on nothing.
            undone = _n_undone(p)
            return (;
                complete=undone == 0,
                remaining=undone,
                done=n_done,
                waited=waited,
                rounds=rounds,
                stopped_by=stopped,
            )
        end

        rounds += 1
        r = run!(p.work_fn, p.vault, p.keys; opts=o, load=load)
        n_done += r.done

        remaining = _n_undone(p)
        remaining == 0 && return (;
            complete=true,
            remaining=0,
            done=n_done,
            waited=waited,
            rounds=rounds,
            stopped_by=nothing,
        )

        # Nothing was completed this round. Either a sibling holds what is left, in which case
        # waiting IS the work, or nobody does and the remainder will not appear.
        if r.done == 0
            if r.busy == 0
                return (;
                    complete=false,
                    remaining=remaining,
                    done=n_done,
                    waited=waited,
                    rounds=rounds,
                    # A round that handed out nothing may have been cut short rather than empty,
                    # and those are different failures: one retries, the other will not.
                    stopped_by=r.stopped_by,
                )
            end
            waited += 1
            sleep(poll)
        end
    end
end

_n_undone(p::Prerequisite) = count(k -> !DataVault.is_done(p.vault, k), p.keys)

# `nothing` is "no bound", so it loses to any real one.
function _tighter(a::Union{Float64,Nothing}, b::Union{Float64,Nothing})
    return a === nothing ? b : (b === nothing ? a : min(a, b))
end

# `p.opts` replaces the stage's knobs wholesale, but not the two that bound the job. Taking the
# tighter deadline rather than the explicit one matters in both directions: reverting it to
# `nothing` lets the barrier outlive the allocation, and so does honouring a more generous ceiling
# the Prerequisite was built with months earlier.
function _merged_opts(p::Prerequisite, opts::RunOpts)::RunOpts
    p.opts === nothing && return opts
    o = p.opts
    job = (
        stop_flag=opts.stop_flag === nothing ? o.stop_flag : opts.stop_flag,
        deadline=_tighter(o.deadline, opts.deadline),
    )
    # Everything else forwarded BY NAME, so a field added to `RunOpts` later cannot be silently
    # reset to its constructor default here.
    return RunOpts(; (f => get(job, f, getfield(o, f)) for f in fieldnames(RunOpts))...)
end

export Prerequisite, run_prerequisite!
