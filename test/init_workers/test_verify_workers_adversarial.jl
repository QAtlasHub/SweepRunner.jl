# ─────────────────────────────────────────────────────────────────────────────
# Adversarial tests for verify_workers! and init_workers! probing.
#
# These tests reproduce the ISSP production scenario that the original
# "happy path" tests missed: compute.jl loads SweepRunner on the
# master, addprocs() spawns workers that do NOT have SweepRunner
# loaded, then calls init_workers!(verbose=true) which invokes
# verify_workers!.
#
# Before the fix, verify_workers!'s @spawnat closure was serialized inside
# the SweepRunner module scope. Workers had no SweepRunner, so
# deserialization threw:
#
#     KeyError: key Base.PkgId(..., "SweepRunner") not found
#
# The fix is to build the probe via `remotecall(Core.eval, p, Main, quote ... end)`
# so the closure lives in the worker's Main module.
# ─────────────────────────────────────────────────────────────────────────────

using SweepRunner, Test
using Distributed, LinearAlgebra

@testset "verify_workers! survives workers without SweepRunner" begin
    # Start from a clean Distributed state
    nprocs() > 1 && rmprocs(workers())
    @test nprocs() == 1

    # Spawn 2 workers with ONLY the current project — critically, we do
    # NOT `@everywhere using SweepRunner`. This mirrors the ISSP
    # Slurm case where init_workers! adds SlurmManager workers before
    # compute.jl could possibly have loaded anything on them.
    project = dirname(Base.active_project())
    addprocs(2; exeflags="--project=$project")
    @test nprocs() == 3

    # Sanity: workers really don't have SweepRunner
    worker_has_pm = remotecall_fetch(workers()[1]) do
        return Base.find_package("SweepRunner") !== nothing && haskey(
            Base.loaded_modules,
            Base.PkgId(Base.UUID("be946ad2-3cb3-4b6e-8f7e-4a5ecc3c255b"), "SweepRunner"),
        )
    end
    @test !worker_has_pm

    # This is what used to crash: the verify_workers! probe must not
    # close over SweepRunner symbols.
    try
        # Workers need LinearAlgebra so BLAS.get_num_threads resolves.
        # In normal flow, init_workers!'s _apply_blas does this; we
        # replicate it here so the test focuses on verify_workers!'s
        # closure scope, not BLAS loading.
        @everywhere workers() Core.eval(Main, :(using LinearAlgebra))

        SweepRunner.verify_workers!()
        @test true  # reached here → probe serialized/deserialized cleanly
    catch e
        @test false
        @error "verify_workers! crashed on SweepRunner-less workers" exception=(
            e, catch_backtrace()
        )
    finally
        rmprocs(workers())
    end
end

@testset "verify_workers!: BLAS threads > 1 emits warning" begin
    nprocs() > 1 && rmprocs(workers())
    project = dirname(Base.active_project())
    addprocs(1; exeflags="--project=$project")

    try
        @everywhere workers() Core.eval(Main, :(using LinearAlgebra))
        # Deliberately set a high BLAS thread count on the worker
        @everywhere workers() LinearAlgebra.BLAS.set_num_threads(4)

        # Capture warnings via Test.@test_logs
        @test_logs (:warn, r"BLAS threads=4") SweepRunner.verify_workers!()
    finally
        rmprocs(workers())
    end
end

@testset "verify_workers!: no-op on single process" begin
    # No workers → returns nothing without printing or erroring
    @test nprocs() == 1
    @test SweepRunner.verify_workers!() === nothing
end
