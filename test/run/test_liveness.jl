# holder_liveness: asking whether the holder is gone, instead of waiting to find out.
#
# `stale_after` is the fallback and stays correct on its own. This only removes the WAIT, and only
# where the answer can be had. Every uncertain path must read `:unknown`: a false `:dead` hands a
# live master's key to someone else, which is what the lock exists to prevent.

using SweepRunner, Test, DataVault, ParamIO, JSON3

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
        SweepRunner._squeue_cache[] = (time(), Set(["1", "12345", "777_3", "777_4"]))
        withenv("SLURM_JOB_ID" => "1") do
            @test holder_liveness("h:1:ab:slurm12345") === :alive     # plain job, queued
            @test holder_liveness("h:1:ab:slurm777_3") === :alive     # array TASK, as squeue names it
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

@testset "owner_token: an array task stamps the id squeue PRINTS, not SLURM_JOB_ID" begin
    # Found in review, and the previous test could not have caught it: it hand-wrote the token with
    # the ARRAY id, sharing the implementation's assumption about what `SLURM_JOB_ID` holds.
    #
    # Slurm gives every task of an array its own raw `SLURM_JOB_ID` (36, 37, 38 ...) while the
    # queue lists them as `<SLURM_ARRAY_JOB_ID>_<SLURM_ARRAY_TASK_ID>` (36_0, 36_1 ...). Stamping
    # the raw id makes every task but the first unfindable, and unfindable reads as `:dead`.
    withenv(
        "SLURM_JOB_ID" => "38", "SLURM_ARRAY_JOB_ID" => "36", "SLURM_ARRAY_TASK_ID" => "2"
    ) do
        @test occursin(":slurm36_2", owner_token())
        @test !occursin(":slurm38", owner_token())
    end
    # A plain job has no array variables and keeps using SLURM_JOB_ID.
    withenv(
        "SLURM_JOB_ID" => "4242",
        "SLURM_ARRAY_JOB_ID" => nothing,
        "SLURM_ARRAY_TASK_ID" => nothing,
    ) do
        @test occursin(":slurm4242", owner_token())
    end
end

@testset "holder_liveness: a queue that cannot see US is not evidence about anyone" begin
    # A `squeue` pointed at another cluster answers successfully and lists none of our ids. Every
    # holder would then be absent, and absent would mean `:dead`.
    saved = SweepRunner._squeue_cache[]
    try
        withenv("SLURM_JOB_ID" => "1", "SLURM_ARRAY_JOB_ID" => nothing) do
            SweepRunner._squeue_cache[] = (time(), Set(["9001", "9002"]))   # our "1" is absent
            @test holder_liveness("h:1:ab:slurm9999") === :unknown
            # The control: the identical call becomes :dead once the queue does list us.
            SweepRunner._squeue_cache[] = (time(), Set(["1", "9001"]))
            @test holder_liveness("h:1:ab:slurm9999") === :dead
        end
    finally
        SweepRunner._squeue_cache[] = saved
    end
end

# A `squeue` on PATH that prints what we tell it to. The real one on the machine this was written
# on is `exec ssh -o BatchMode=yes <front> -- squeue`, so calling it from a test would make a live
# network call on every CI run, and CI runs on that same machine.
function with_fake_squeue(f, output::AbstractString; exitcode::Int=0, sleep_for::Real=0)
    dir = mktempdir()
    try
        bin = joinpath(dir, "squeue")
        write(
            bin,
            """
            #!/bin/sh
            [ $(sleep_for) = 0 ] || sleep $(sleep_for)
            printf '%s' '$(output)'
            exit $(exitcode)
            """,
        )
        chmod(bin, 0o755)
        withenv("PATH" => dir * ":" * get(ENV, "PATH", "")) do
            return f()
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end

_uncached() = (SweepRunner._squeue_cache[] = (-Inf, nothing))

@testset "_live_slurm_jobs: parses the shapes squeue -o %i actually prints" begin
    # The only value-level test of the real fetch-and-parse. Every other testset seeds the cache,
    # so the parser itself never runs in them: a regression in the `-o` format or the split would
    # go unnoticed.
    saved = SweepRunner._squeue_cache[]
    try
        _uncached()
        out = with_fake_squeue("623186\n623186_1\n624087_[0-1]\n595411_[0-3%4]\n") do
            SweepRunner._live_slurm_jobs()
        end
        @test out == Set(["623186", "623186_1", "624087_[0-1]", "595411_[0-3%4]"])

        _uncached()
        @test with_fake_squeue("") do
            SweepRunner._live_slurm_jobs()
        end == Set(String[])                       # an empty queue is a real answer, not an error
    finally
        SweepRunner._squeue_cache[] = saved
    end
end

@testset "_live_slurm_jobs: a failing or absent squeue is no opinion, never an empty queue" begin
    # `nothing` and `Set()` must not be confused: an empty queue says every holder is dead, while
    # no opinion falls back to stale_after.
    saved = SweepRunner._squeue_cache[]
    try
        _uncached()
        @test with_fake_squeue("garbage"; exitcode=1) do
            SweepRunner._live_slurm_jobs()
        end === nothing

        _uncached()
        @test withenv("PATH" => "") do             # squeue not on PATH at all
            SweepRunner._live_slurm_jobs()
        end === nothing
    finally
        SweepRunner._squeue_cache[] = saved
    end
end

@testset "_squeue_output: a hanging squeue is killed, not waited on" begin
    # It runs on the hot path of every contended key, under the cache lock, and the real binary can
    # be a wrapper around a network call. Neither stop_flag nor deadline is read inside a key.
    t0 = time()
    out = with_fake_squeue("never"; sleep_for=30) do
        SweepRunner._squeue_output(1.0)
    end
    elapsed = time() - t0
    @test out === nothing                          # timed out: no opinion
    @test elapsed < 15.0                           # and did not sit there for the full 30 s
end

@testset "holder_liveness: a hostname beginning with slurm is not a job id" begin
    # Found in review. The Slurm field is written by `owner_token` as the FOURTH part; scanning
    # every part for a `slurm` prefix matched the HOSTNAME, read `-node-01` out of it as a job id,
    # found it absent from the queue and answered `:dead` for a master that was alive. Clusters
    # name nodes `slurm*` often enough that this is a real configuration, and a false `:dead` is
    # the one answer that costs a double execution.
    saved = SweepRunner._squeue_cache[]
    try
        SweepRunner._squeue_cache[] = (time(), Set(["1", "777"]))
        withenv("SLURM_JOB_ID" => "1") do
            @test holder_liveness("slurm-node-01:12345:ab01cd23") === :unknown
            @test holder_liveness("slurm:1:ab") === :unknown
            # A job id Slurm could never issue is not looked up either: "absent from the queue"
            # must not be the answer for a corrupt `.running`.
            @test holder_liveness("h:1:ab:slurm-node-01") === :unknown
            @test holder_liveness("h:1:ab:slurm" * "\u3042") === :unknown
            # ...while a well-formed one on the same code path still resolves both ways.
            @test holder_liveness("h:1:ab:slurm777") === :alive
            @test holder_liveness("h:1:ab:slurm999") === :dead
        end
    finally
        SweepRunner._squeue_cache[] = saved
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

@testset "reaping emits :lock_reaped, naming the owner it cleared" begin
    # `:gave_up` is asserted from the JSONL elsewhere; this kind had no such check, so a reap that
    # silently stopped logging would look identical to one that worked.
    with_one_left() do v, ks
        log = EventLog(joinpath(v.outdir, "reap.jsonl"))
        @test DataVault.acquire_running!(v, ks[1], _DEAD) === :ok
        @test SweepRunner._reap_if_dead!(v, ks[1], :liv, log) == true

        recs = [JSON3.read(l) for l in readlines(log.path)]
        reaped = [r for r in recs if String(r["kind"]) == "lock_reaped"]
        @test length(reaped) == 1
        @test String(reaped[1]["owner"]) == _DEAD
        @test String(reaped[1]["key"]) == ParamIO.canonical(ks[1])
    end
end

@testset "holder_liveness: Slurm evidence outranks the pid on the same host" begin
    # Every other Slurm test uses a foreign hostname, so the two branches are only ever exercised
    # apart. Here they disagree: the pid is this live process, the job is absent from the queue.
    # Slurm must win, because it can see across the whole allocation and /proc cannot.
    saved = SweepRunner._squeue_cache[]
    try
        withenv("SLURM_JOB_ID" => "1", "SLURM_ARRAY_JOB_ID" => nothing) do
            SweepRunner._squeue_cache[] = (time(), Set(["1"]))
            tok = "$(_HOST):$(getpid()):abcd:slurm999999"
            @test holder_liveness("$(_HOST):$(getpid()):abcd") === :alive   # pid alone: alive
            @test holder_liveness(tok) === :dead                            # with Slurm: dead
        end
    finally
        SweepRunner._squeue_cache[] = saved
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
