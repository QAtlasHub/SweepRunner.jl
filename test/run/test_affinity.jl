# affinity (#47): a free worker prefers a key whose group it has already handled.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed

const _AFF_CFG = joinpath(@__DIR__, "fixtures", "affinity.toml")

# How many times a worker's stream of keys changes group. That is the quantity affinity exists to
# lower: each change is a group the worker's memo does not have.
function _transitions(pairs)
    per = Dict{Int,Vector{Any}}()
    for (pid, g) in pairs
        push!(get!(Vector{Any}, per, pid), g)
    end
    return sum(count(i -> v[i] != v[i - 1], 2:length(v)) for v in values(per); init=0)
end

group_of(k) = ParamIO.param(k, "L")

# Each key appends "<pid> <group>" so the master can reconstruct who ran what, in order.
#
# Written the way `log_event` writes: ONE `write` to an unbuffered O_APPEND descriptor. An
# `open(trace, "a") do io; println(io, ...)` here is a buffered `IOStream`, and across the worker
# processes it drops lines; the tell is a trace one entry short of the keys that actually ran.
function _make_work(trace)
    return k -> begin
        line = "$(Distributed.myid()) $(ParamIO.param(k, "L"))\n"
        fd = Base.Filesystem.open(
            trace,
            Base.Filesystem.JL_O_WRONLY | Base.Filesystem.JL_O_CREAT |
            Base.Filesystem.JL_O_APPEND,
            0o644,
        )
        try
            write(fd, codeunits(line))
        finally
            close(fd)
        end
        sleep(0.01)
        return Dict{String,Any}("x" => 1)
    end
end

function _read_trace(f)
    return [
        (parse(Int, split(l)[1]), split(l)[2]) for
        l in (isfile(f) ? readlines(f) : String[])
    ]
end

function with_workers(f, n)
    nprocs() > 1 && rmprocs(workers())
    addprocs(n; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        f()
    finally
        rmprocs(workers())
    end
end

@testset "affinity: a worker changes group far less often than without it" begin
    with_workers(4) do
        off_t = on_t = 0
        outdir = mktempdir()
        try
            trace = joinpath(outdir, "off.txt")
            v = DataVault.Vault(_AFF_CFG; run="off", outdir=outdir)
            run!(_make_work(trace), v, DataVault.keys(v))
            off = _read_trace(trace)
            off_t = _transitions(off)

            trace2 = joinpath(outdir, "on.txt")
            v2 = DataVault.Vault(_AFF_CFG; run="on", outdir=outdir)
            run!(_make_work(trace2), v2, DataVault.keys(v2); affinity=group_of)
            on = _read_trace(trace2)
            on_t = _transitions(on)

            # Same work done either way.
            @test length(off) == length(DataVault.keys(v))
            @test length(on) == length(DataVault.keys(v2))
            @test all(k -> DataVault.is_done(v2, k), DataVault.keys(v2))

            @info "affinity group transitions" without = off_t with = on_t
            @test on_t < off_t
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "affinity: it is a preference, so no worker sits idle and no group is serialised" begin
    with_workers(4) do
        outdir = mktempdir()
        try
            trace = joinpath(outdir, "t.txt")
            v = DataVault.Vault(_AFF_CFG; run="pref", outdir=outdir)
            run!(_make_work(trace), v, DataVault.keys(v); affinity=group_of)
            t = _read_trace(trace)

            nworkers_used = length(unique(pid for (pid, _) in t))
            ngroups = length(unique(g for (_, g) in t))
            @test ngroups == 2

            # An exclusive partition of 2 groups can never put more than 2 workers to work, so
            # more workers than groups IS the statement that this is a preference. The fixture is
            # built to make that distinguishable: 4 workers, 2 groups.
            @test nworkers_used > ngroups
            @test nworkers_used == 4          # and in fact none of them idled
            @test length(t) == length(DataVault.keys(v))
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "affinity: every key runs exactly once, as without it" begin
    with_workers(3) do
        outdir = mktempdir()
        try
            trace = joinpath(outdir, "t.txt")
            v = DataVault.Vault(_AFF_CFG; run="once", outdir=outdir)
            r = run!(_make_work(trace), v, DataVault.keys(v); affinity=group_of)
            t = _read_trace(trace)
            @test r.done == length(DataVault.keys(v))
            @test r.err == 0
            @test length(t) == length(DataVault.keys(v))
            @test all(k -> DataVault.is_done(v, k), DataVault.keys(v))
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "affinity: a throwing work_fn is reported, not swallowed" begin
    with_workers(2) do
        outdir = mktempdir()
        try
            v = DataVault.Vault(_AFF_CFG; run="bad", outdir=outdir)
            r = run!(
                k -> error("boom"),
                v,
                DataVault.keys(v);
                opts=RunOpts(max_attempts=1),
                affinity=group_of,
            )
            @test r.done == 0
            @test r.err == length(DataVault.keys(v))
            @test !any(k -> DataVault.is_done(v, k), DataVault.keys(v))
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "affinity: a worker that dies hands its key back instead of losing it" begin
    # The fault tolerance `pmap` gives through `retry_check`, which this dispatcher has to
    # reproduce by hand. One key kills its worker outright with `_exit`, so remotecall_fetch sees
    # ProcessExitedException rather than a caught error.
    with_workers(3) do
        outdir = mktempdir()
        try
            v = DataVault.Vault(_AFF_CFG; run="died", outdir=outdir)
            ks = DataVault.keys(v)
            victim = ParamIO.canonical(ks[1])
            work = k -> begin
                if ParamIO.canonical(k) == victim
                    ccall(:_exit, Cvoid, (Cint,), 1)
                end
                sleep(0.01)
                return Dict{String,Any}("x" => 1)
            end

            # stale_after short enough that the dead worker's abandoned .running is reclaimable on
            # the next pass; without that the key stays :busy for its whole duration.
            o = RunOpts(; stale_after=2.0, heartbeat_interval=1.0)
            r = run!(work, v, ks; opts=o, affinity=group_of)

            # It returned rather than hanging, and the loss was not counted as an error.
            @test r.done == length(ks) - 1
            @test r.err == 0
            @test !DataVault.is_done(v, ks[1])

            # And the key is retriable: a later pass, once the abandoned lock is stale, gets it.
            sleep(2.2)
            r2 = run!(k -> Dict{String,Any}("x" => 1), v, ks; opts=o, affinity=group_of)
            @test r2.done == 1
            @test all(k -> DataVault.is_done(v, k), ks)
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "affinity: nothing is the default and leaves the pmap path alone" begin
    with_workers(2) do
        outdir = mktempdir()
        try
            v = DataVault.Vault(_AFF_CFG; run="default", outdir=outdir)
            r = run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v))
            @test r.done == length(DataVault.keys(v))
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end
