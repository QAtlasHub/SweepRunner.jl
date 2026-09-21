# Artifacts: a key whose artifact is mid-build is deferred, not failed; and keys group by artifact.

using SweepRunner, Test, DataVault, ParamIO, JSON3

function _ar_config(dir)
    path = joinpath(dir, "config.toml")
    write(
        path,
        """
        [study]
        project_name = "ar_study"
        [datavault]
        path_keys = ["run.U", "run.omega1"]
        [[paramsets]]
        [paramsets.run]
        U      = [0.0, 0.2]
        omega1 = [1.0, 2.0, 3.0]
        [artifacts.gs]
        depends_on = ["U"]
        """,
    )
    return path
end

function with_ar(f)
    dir = mktempdir()
    try
        f(DataVault.Vault(_ar_config(dir); outdir=joinpath(dir, "out"), run="a"), dir)
    finally
        rm(dir; recursive=true, force=true)
    end
end

function _events(dir)
    return [
        JSON3.read(l) for
        f in readdir(joinpath(dir, "out"); join=true) if endswith(f, ".jsonl") for
        l in eachline(f) if !isempty(l)
    ]
end

# Hold the build lock of the artifact `k` needs, as another job mid-build would.
function _hold!(v, k)
    lock = joinpath(DataVault.artifact_dir(v, :gs, k), ".building")
    tok = new_owner_token()
    @assert DataVault._acquire_lock_at!(lock, tok) === :ok
    return lock, tok
end

@testset "artifacts: a key whose artifact is mid-build is deferred, then done, at no attempt" begin
    with_ar() do v, dir
        keys = DataVault.keys(v)
        held = first(filter(k -> param(k, "run.U") == 0.2, keys))
        lock, tok = _hold!(v, held)
        # The "other job" finishes its build once all three U = 0.2 keys have been turned away —
        # counted, not timed, so a slow first compile cannot release it before they arrive.
        busy = Ref(0)
        work_fn =
            k -> begin
                gs = try
                    DataVault.artifact!(a -> param(a, "run.U"), v, :gs, k; wait=false)
                catch e
                    e isa DataVault.ArtifactBusy &&
                        (busy[] += 1) == 3 &&
                        DataVault._clear_lock_at!(lock, tok)
                    rethrow()
                end
                Dict{String,Any}("x" => gs + param(k, "run.omega1"))
            end
        # max_attempts = 1: a deferral that spent an attempt would end these keys as :error.
        r = run!(
            work_fn,
            v,
            keys;
            opts=RunOpts(
                workers=:sequential, max_attempts=1, defer_poll=0.5, stop_flag=nothing
            ),
        )
        @test r.done == length(keys)
        @test r.err == 0
        @test r.busy == 0
        @test all(k -> DataVault.is_done(v, k), keys)
        kinds = [String(e.kind) for e in _events(dir)]
        @test count(==("artifact_busy"), kinds) >= 3          # the three U = 0.2 keys, at least once
        @test "deferred_round" in kinds
    end
end

@testset "artifacts: a key still deferred when the run stops is busy, not failed" begin
    with_ar() do v, dir
        keys = DataVault.keys(v)
        held = first(filter(k -> param(k, "run.U") == 0.2, keys))
        _hold!(v, held)                                         # never released
        flag = joinpath(dir, "STOP")
        # Raise the stop flag at the first deferral, as a job reaching its deadline would.
        work_fn =
            k -> Dict{String,Any}(
                "x" => try
                    DataVault.artifact!(a -> 1.0, v, :gs, k; wait=false)
                catch e
                    e isa DataVault.ArtifactBusy && touch(flag)
                    rethrow()
                end
            )
        r = run!(
            work_fn,
            v,
            keys;
            opts=RunOpts(
                workers=:sequential, max_attempts=1, defer_poll=0.3, stop_flag=flag
            ),
        )
        @test r.done == 3                                       # U = 0.0
        # U = 0.2: the first is deferred, and counted with `busy`; the sequential path does not
        # dispatch the rest after the flag, and (as before this change) counts them nowhere.
        @test r.busy >= 1
        @test r.err == 0
        @test !any(k -> param(k, "run.U") == 0.2 && DataVault.is_done(v, k), keys)
        @test r.stopped_by === :flag
    end
end

@testset "artifacts: artifact_affinity groups keys by the artifact they need" begin
    with_ar() do v, dir
        aff = artifact_affinity(v, :gs)
        keys = DataVault.keys(v)
        groups = Dict{Any,Vector{Float64}}()
        for k in keys
            push!(get!(groups, aff(k), Float64[]), param(k, "run.U"))
        end
        @test length(groups) == 2
        @test all(us -> length(unique(us)) == 1 && length(us) == 3, values(groups))
    end
end
