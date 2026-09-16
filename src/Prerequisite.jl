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

`stop_flag` and `deadline` are NOT overridden by omission. They say when the job must stop rather
than how this stage runs, so leaving either unset here inherits the caller's; setting one takes
precedence as any other field does. Without that, raising `stale_after` alone silently dropped the
caller's deadline and let the barrier outlive the allocation running it.

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
            return (;
                complete=false,
                remaining=_n_undone(p),
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

# `p.opts` replaces the stage's knobs wholesale. `stop_flag` and `deadline` are not stage knobs:
# they bound the JOB. An unset one therefore inherits the caller's rather than reverting to the
# `RunOpts` default, which for `deadline` is `nothing` — no bound at all.
function _merged_opts(p::Prerequisite, opts::RunOpts)::RunOpts
    p.opts === nothing && return opts
    o = p.opts
    return RunOpts(;
        workers=o.workers,
        max_attempts=o.max_attempts,
        stale_after=o.stale_after,
        heartbeat_interval=o.heartbeat_interval,
        stop_flag=o.stop_flag === nothing ? opts.stop_flag : o.stop_flag,
        log_level=o.log_level,
        deadline=o.deadline === nothing ? opts.deadline : o.deadline,
    )
end

export Prerequisite, run_prerequisite!
