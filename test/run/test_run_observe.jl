# Each `.done` names the source observation of the process that computed its key.

using SweepRunner, Test, DataVault, ParamIO, Distributed, TOML

const _OBSRUN_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _obsrun_done(v, key)
    pairs = (split(l, '='; limit=2) for l in eachline(DataVault._done_file(v, key)))
    return Dict(String(p[1]) => String(p[2]) for p in pairs)
end
_obsrun_dir(v) = joinpath(v.outdir, ".datavault", v.spec.study.project_name)
function _obsrun_record(v, token)
    return TOML.parsefile(joinpath(_obsrun_dir(v), "observations", "$token.toml"))
end
_obsrun_work(key) = Dict{String,Any}("N" => key.params["N"])

function with_obsrun_vault(f, run)
    outdir = mktempdir()
    try
        v = DataVault.Vault(_OBSRUN_CFG; run=run, outdir=outdir)
        f(v, ParamIO.expand(v.spec))
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "run!: every .done names the master's observation when nothing fans out" begin
    with_obsrun_vault("observe") do v, keys
        res = run!(_obsrun_work, v, keys; opts=RunOpts(; workers=:sequential))
        @test res.done == length(keys)
        tokens = unique(_obsrun_done(v, k)["observation"] for k in keys)
        @test length(tokens) == 1 && startswith(only(tokens), "obs2-")
        r = _obsrun_record(v, only(tokens))
        @test r["phase"] == "run-start"
        @test r["process"]["role"] == "master" && r["process"]["pid"] == getpid()
        @test isfile(joinpath(_obsrun_dir(v), "sources", r["source"], "COMPLETE"))
        # The work function is the entry code; defined in this test, not a package, it cannot be
        # vouched for, and the observation says so rather than claiming a match.
        @test only(r["code"])["name"] == "_obsrun_work"
        @test r["binding"] == "unverified"
        @test any(contains("_obsrun_work"), r["binding_reasons"])
    end
end

@testset "run!: observe=false writes unknown, never an earlier token" begin
    with_obsrun_vault("no-observe") do v, keys
        SweepRunner._observe_here!(v, "master")       # a token left from before
        run!(_obsrun_work, v, keys; opts=RunOpts(; workers=:sequential), observe=false)
        @test all(_obsrun_done(v, k)["observation"] == "unknown" for k in keys)
    end
end

@testset "run!: an observation that fails does not stop the run, and says why" begin
    with_obsrun_vault("observe-fails") do v, keys
        mkpath(_obsrun_dir(v))
        write(
            joinpath(_obsrun_dir(v), "sources"), "a file where the snapshot store should be"
        )
        res = run!(_obsrun_work, v, keys; opts=RunOpts(; workers=:sequential))
        @test res.done == length(keys)
        @test all(_obsrun_done(v, k)["observation"] == "unknown" for k in keys)
        logs = filter(f -> startswith(f, "events_"), readdir(v.outdir))
        lines = vcat((readlines(joinpath(v.outdir, f)) for f in logs)...)
        @test any(l -> occursin("observe_failed", l), lines)
    end
end

@testset "run! distributed: each marker carries the token of the worker that computed it" begin
    proj = dirname(Base.active_project())
    pids = addprocs(2; exeflags="--project=$proj")
    try
        with_obsrun_vault("observe-workers") do v, keys
            # A closure, not a named function: a function defined only on the master does not
            # exist on the workers.
            res = run!(key -> Dict{String,Any}("N" => key.params["N"]), v, keys)
            @test res.done == length(keys)
            recs = [
                _obsrun_record(v, t) for
                t in unique(_obsrun_done(v, k)["observation"] for k in keys)
            ]
            @test all(r -> r["process"]["role"] == "worker", recs)
            @test all(r -> r["process"]["myid"] in pids, recs)
            # A closure is the master's code on the worker: never a match.
            @test all(r -> r["binding"] == "unverified", recs)
            @test all(
                r -> any(contains("arrived from another process"), r["binding_reasons"]),
                recs,
            )
            roles = [
                TOML.parsefile(joinpath(_obsrun_dir(v), "observations", f))["process"]["role"]
                for f in readdir(joinpath(_obsrun_dir(v), "observations"))
            ]
            @test count(==("master"), roles) == 1 && count(==("worker"), roles) == 2
        end
    finally
        rmprocs(pids)
    end
end
