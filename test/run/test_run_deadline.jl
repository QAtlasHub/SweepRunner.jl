# deadline (#43) and the durable per-key claim record (#44).

using SweepRunner, Test, DataVault, ParamIO, JSON3

const FIXTURE_CFG_D = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_d(f; run::AbstractString="deadline")
    outdir = mktempdir()
    try
        f(DataVault.Vault(FIXTURE_CFG_D; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _events_d(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

_kinds_d(outdir) = [String(e["kind"]) for e in _events_d(outdir)]

@testset "deadline: a deadline already past hands out no key" begin
    with_vault_d() do v, outdir
        n = Ref(0)
        work = k -> (n[] += 1; Dict{String,Any}("x" => 1))
        r = run!(work, v, ParamIO.expand(v.spec); opts=RunOpts(deadline=time() - 1))
        @test n[] == 0
        @test r.done == 0
        @test r.stopped_by === :deadline
    end
end

@testset "deadline: a deadline in the future does not stop anything" begin
    # Control for the testset above: the same call with the deadline moved forward must run every
    # key, so `done == 0` there is the deadline and not the fixture.
    with_vault_d() do v, outdir
        keys = ParamIO.expand(v.spec)
        n = Ref(0)
        work = k -> (n[] += 1; Dict{String,Any}("x" => 1))
        r = run!(work, v, keys; opts=RunOpts(deadline=time() + 3600))
        @test n[] == length(keys)
        @test r.done == length(keys)
        @test r.stopped_by === nothing
    end
end

@testset "deadline: it stops BETWEEN keys, not inside one" begin
    # The documented granularity. A key already in work_fn runs to completion, so the deadline
    # bounds when dispatching stops and not when run! returns.
    with_vault_d() do v, outdir
        keys = ParamIO.expand(v.spec)
        @test length(keys) > 1
        started = Ref(0)
        deadline = time() + 0.3
        work = k -> (started[] += 1; sleep(0.6); Dict{String,Any}("x" => 1))
        r = run!(work, v, keys; opts=RunOpts(workers=:sequential, deadline=deadline))
        @test started[] >= 1                 # the first key ran
        @test started[] < length(keys)       # later keys were not handed out
        @test time() > deadline              # and the return is past it, by that first key
        @test r.stopped_by === :deadline
    end
end

@testset "deadline: the flag still wins, and is reported as itself" begin
    with_vault_d() do v, outdir
        stop = joinpath(outdir, "STOP_NOW")
        touch(stop)
        r = run!(
            k -> Dict{String,Any}("x" => 1),
            v,
            ParamIO.expand(v.spec);
            opts=RunOpts(stop_flag=stop, deadline=time() + 3600),
        )
        @test r.done == 0
        @test r.stopped_by === :flag
    end
end

@testset "deadline: RunOpts accepts an Int, and nothing is the default" begin
    @test RunOpts().deadline === nothing
    @test RunOpts(; deadline=1).deadline === 1.0
end

@testset "key_acquired: every key this master claimed is on disk, at :info" begin
    with_vault_d() do v, outdir
        keys = ParamIO.expand(v.spec)
        run!(k -> Dict{String,Any}("x" => 1), v, keys)
        ev = _events_d(outdir)
        acquired = [e for e in ev if String(e["kind"]) == "key_acquired"]
        @test length(acquired) == length(keys)
        @test Set(String(e["key"]) for e in acquired) ==
            Set(ParamIO.canonical(k) for k in keys)
        @test all(String(e["acq"]) in ("ok", "reclaimed") for e in acquired)
        # :info, not :debug: the default log level must carry it, which is the whole point.
        @test "key_start" ∉ _kinds_d(outdir)
    end
end

@testset "key_acquired: a key whose work throws is still recorded as claimed" begin
    # The systematic-failure shape of #44: one axis value throws on every key. The status tree
    # shows only the successes, so the claim record is what makes the gap attributable.
    with_vault_d() do v, outdir
        keys = ParamIO.expand(v.spec)
        bad = k -> ParamIO.param(k, "N") == 8 ? error("boom") : Dict{String,Any}("x" => 1)
        r = run!(bad, v, keys; opts=RunOpts(max_attempts=1))
        @test r.err > 0
        @test r.done > 0

        ev = _events_d(outdir)
        acquired = Set(String(e["key"]) for e in ev if String(e["kind"]) == "key_acquired")
        @test length(acquired) == length(keys)

        failed = [k for k in keys if ParamIO.param(k, "N") == 8]
        @test !isempty(failed)
        for k in failed
            kc = ParamIO.canonical(k)
            @test kc ∈ acquired                       # claimed
            @test !DataVault.is_done(v, k)            # and left nothing in the status tree
        end
    end
end
