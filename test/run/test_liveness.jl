# holder_liveness: asking whether the holder is gone, instead of waiting to find out.
#
# `stale_after` is the fallback and stays correct on its own. This only removes the WAIT, and only
# where the answer can be had. Every uncertain path must read `:unknown`: a false `:dead` hands a
# live master's key to someone else, which is what the lock exists to prevent.

using SweepRunner, Test, DataVault, ParamIO

const _LIV_CFG = joinpath(@__DIR__, "fixtures", "study.toml")
const _HOST = gethostname()
const _DEAD = "$(_HOST):999999:deadbeef"      # a pid that cannot be running

@testset "holder_liveness: :dead only on positive evidence" begin
    @test holder_liveness(owner_token()) === :alive      # this very process
    @test holder_liveness(_DEAD) === :dead               # /proc says no such pid, on this host

    # Everything uncertain is :unknown, so stale_after decides exactly as before.
    @test holder_liveness("otherhost:123:abcd") === :unknown       # another machine
    @test holder_liveness("garbage") === :unknown
    @test holder_liveness("") === :unknown
    @test holder_liveness("$(_HOST):notanumber:abcd") === :unknown
end

@testset "holder_liveness: the queue decision, with the fetch stubbed" begin
    # The membership rule cannot be reached without a scheduler, and it is the part with real
    # content: an array task is `12345_7` in the queue while `SLURM_JOB_ID` is `12345`, so a job is
    # alive if ANY of its tasks is. Only the squeue CALL is stubbed; the decision under test is the
    # package's own.
    saved = SweepRunner._squeue_cache[]
    try
        SweepRunner._squeue_cache[] = (time(), Set(["12345", "777_3", "777_4"]))
        withenv("SLURM_JOB_ID" => "1") do
            @test holder_liveness("h:1:ab:slurm12345") === :alive     # plain job, queued
            @test holder_liveness("h:1:ab:slurm777") === :alive       # array job, a task queued
            @test holder_liveness("h:1:ab:slurm999") === :dead        # absent: finished or killed
        end

        # No opinion from squeue is never :dead, however long ago it was asked.
        SweepRunner._squeue_cache[] = (time(), nothing)
        withenv("SLURM_JOB_ID" => "1") do
            @test holder_liveness("h:1:ab:slurm999") === :unknown
        end
    finally
        SweepRunner._squeue_cache[] = saved   # never leak a stub into a sibling test file
    end
end

@testset "holder_liveness: outside an allocation the queue is not consulted at all" begin
    saved = SweepRunner._squeue_cache[]
    try
        SweepRunner._squeue_cache[] = (time(), Set(String[]))   # an empty queue: everything absent
        withenv("SLURM_JOB_ID" => nothing) do
            # Would be :dead if the guard were not there, since the job is absent from this queue.
            @test holder_liveness("otherhost:1:ab:slurm12345") === :unknown
        end
    finally
        SweepRunner._squeue_cache[] = saved
    end
end

@testset "the squeue fetch cannot throw and cannot invent a :dead" begin
    # Forces the cache miss so the real call runs. The assertion is the safety contract rather
    # than a value: this executes on a machine with no scheduler (CI), on one whose `squeue` is a
    # wrapper around a remote cluster (the development box), and inside a real allocation, and in
    # every one of those it must come back with an answer rather than an exception.
    saved = SweepRunner._squeue_cache[]
    try
        SweepRunner._squeue_cache[] = (-Inf, nothing)
        live = SweepRunner._live_slurm_jobs()
        @test live === nothing || live isa Set{String}

        SweepRunner._squeue_cache[] = (-Inf, nothing)
        withenv("SLURM_JOB_ID" => "1") do
            @test holder_liveness("h:1:ab:slurm999999") in (:alive, :dead, :unknown)
        end
    finally
        SweepRunner._squeue_cache[] = saved
    end
end

@testset "owner_token: carries the Slurm job so another HOST can ask" begin
    withenv("SLURM_JOB_ID" => "4242") do
        t = owner_token()
        @test occursin(":slurm4242", t)
        @test startswith(t, gethostname() * ":")
    end
    withenv("SLURM_JOB_ID" => nothing) do
        @test !occursin("slurm", owner_token())
    end
end

function with_one_left(f)
    outdir = mktempdir()
    try
        v = DataVault.Vault(_LIV_CFG; run="liv", outdir=outdir)
        ks = DataVault.keys(v)
        for k in ks[2:end]
            DataVault.save!(v, k, Dict("x" => 1))
            DataVault.mark_done!(v, k)
        end
        f(v, ks)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "a dead holder's lock is reaped at once, not after stale_after" begin
    with_one_left() do v, ks
        @test DataVault.acquire_running!(v, ks[1], _DEAD) === :ok
        @test DataVault.running_owner(v, ks[1]) == _DEAD

        t0 = time()
        r = run_loop!(
            k -> Dict{String,Any}("x" => 1),
            v,
            ks;
            opts=RunOpts(; workers=:sequential, stale_after=600.0, heartbeat_interval=30.0),
            max_empty_rounds=2,
            idle_sleep=0.5,
        )
        elapsed = time() - t0

        @test DataVault.is_done(v, ks[1])       # completed
        @test r.done == 1
        @test elapsed < 30.0                    # and did NOT wait out the 600 s stale_after
    end
end

@testset "a LIVE holder's lock is not reaped" begin
    # The control, on the reaper DIRECTLY. Going through run_loop! here would prove nothing: with a
    # short stale_after the timeout reclaims the lock legitimately (nothing is heartbeating it in
    # this test), and with a long one the loop just waits. Neither exercises the reaper's decision.
    with_one_left() do v, ks
        log = EventLog(joinpath(v.outdir, "e.jsonl"))
        mine = owner_token()                    # this process: provably alive
        @test DataVault.acquire_running!(v, ks[1], mine) === :ok

        @test SweepRunner._reap_if_dead!(v, ks[1], :liv, log) == false
        @test DataVault.is_running(v, ks[1])
        @test DataVault.running_owner(v, ks[1]) == mine   # untouched

        # And the same reaper DOES clear it once the owner is one that cannot be running, so the
        # `false` above is the liveness answer and not an inert function.
        DataVault.clear_running!(v, ks[1], mine)
        @test DataVault.acquire_running!(v, ks[1], _DEAD) === :ok
        @test SweepRunner._reap_if_dead!(v, ks[1], :liv, log) == true
        @test !DataVault.is_running(v, ks[1])
    end
end

@testset "an unstamped lock is left to the timeout" begin
    # `mark_running!` writes no owner, and a lock that cannot be attributed cannot be judged.
    with_one_left() do v, ks
        DataVault.mark_running!(v, ks[1])
        @test DataVault.running_owner(v, ks[1]) === nothing
        @test SweepRunner._reap_if_dead!(
            v, ks[1], :liv, EventLog(joinpath(v.outdir, "e.jsonl"))
        ) == false
        @test DataVault.is_running(v, ks[1])
    end
end
