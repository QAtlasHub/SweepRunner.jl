# Prerequisite (#46): shared setup as its own stage instead of inside work_fn.

using SweepRunner, Test, DataVault, ParamIO, JSON3

const _PRE_MAIN = joinpath(@__DIR__, "fixtures", "study.toml")
const _PRE_PREP = joinpath(@__DIR__, "fixtures", "prep.toml")

function with_both(f)
    outdir = mktempdir()
    try
        main = DataVault.Vault(_PRE_MAIN; run="dependent", outdir=outdir)
        prep = DataVault.Vault(_PRE_PREP; run="setup", outdir=outdir)
        f(main, prep, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

# One line per build, appended. The point of the issue is HOW MANY times a setup gets built.
_log_build!(path, what) = open(path, "a") do io
    return println(io, what)
end
_n_builds(path, what) = isfile(path) ? count(==(what), readlines(path)) : 0

setup_of(k) = ParamIO.param(k, "N")

@testset "prerequisite: the setup stage finishes before the dependent stage starts" begin
    with_both() do main, prep, outdir
        order = joinpath(outdir, "order.txt")
        prep_fn =
            k -> (_log_build!(order, "prep"); Dict{String,Any}("state" => setup_of(k)))
        work_fn = k -> (_log_build!(order, "work"); Dict{String,Any}("x" => 1))

        pkeys = DataVault.keys(prep)
        r = run_loop!(
            work_fn,
            main,
            DataVault.keys(main);
            prerequisite=Prerequisite(prep_fn, prep, pkeys),
            opts=RunOpts(workers=:sequential),
            idle_sleep=0.0,
        )

        @test r.ran
        @test r.prerequisite.complete
        lines = readlines(order)
        @test count(==("prep"), lines) == length(pkeys)
        @test count(==("work"), lines) == length(DataVault.keys(main))
        # Every prep precedes every work: the barrier, stated as an ordering.
        @test findlast(==("prep"), lines) < findfirst(==("work"), lines)
    end
end

@testset "prerequisite: the setup is built once per setup key, not once per dependent key" begin
    with_both() do main, prep, outdir
        builds = joinpath(outdir, "builds.txt")
        prep_fn = k -> (_log_build!(builds, "N$(setup_of(k))"); Dict{String,Any}("s" => 1))
        work_fn = k -> Dict{String,Any}("x" => 1)

        run_loop!(
            work_fn,
            main,
            DataVault.keys(main);
            prerequisite=Prerequisite(prep_fn, prep, DataVault.keys(prep)),
            opts=RunOpts(workers=:sequential),
            idle_sleep=0.0,
        )

        # 4 dependent keys fall onto 2 setups.
        @test length(DataVault.keys(main)) == 4
        @test _n_builds(builds, "N4") == 1
        @test _n_builds(builds, "N8") == 1
    end
end

@testset "prerequisite: without one, the same setup IS rebuilt per dependent key" begin
    # The control. The check-then-build idiom this replaces, in one process: work_fn builds the
    # setup when it is not on disk. Without the prerequisite stage the fixture MUST duplicate, or
    # the testset above passes for having nothing to prevent.
    with_both() do main, prep, outdir
        builds = joinpath(outdir, "builds.txt")
        cache = joinpath(outdir, "cache")
        mkpath(cache)
        function work_fn(k)
            f = joinpath(cache, "N$(setup_of(k)).state")
            if !isfile(f)                       # check-then-build
                _log_build!(builds, "N$(setup_of(k))")
                write(f, "1")
            end
            return Dict{String,Any}("x" => 1)
        end

        # Two masters interleaved the way separate processes are: each sees the cache as it was
        # before the other wrote, which is the race that made 31 workers produce 5 states.
        for k in DataVault.keys(main)
            rm(cache; recursive=true, force=true)
            mkpath(cache)
            work_fn(k)
        end
        @test _n_builds(builds, "N4") == 2       # duplicated: 2 dependent keys per setup
        @test _n_builds(builds, "N8") == 2
    end
end

@testset "prerequisite: a setup that cannot be built blocks the dependent stage" begin
    with_both() do main, prep, outdir
        ran = Ref(0)
        r = run_loop!(
            k -> (ran[] += 1; Dict{String,Any}("x" => 1)),
            main,
            DataVault.keys(main);
            prerequisite=Prerequisite(
                k -> error("cannot cool"), prep, DataVault.keys(prep)
            ),
            opts=RunOpts(workers=:sequential, max_attempts=1),
            idle_sleep=0.0,
        )
        @test !r.ran
        @test ran[] == 0
        @test !r.prerequisite.complete
        @test r.prerequisite.remaining == length(DataVault.keys(prep))
    end
end

@testset "prerequisite: a live sibling's lock is WAITED for, not treated as failure" begin
    # run_loop! would stop after max_empty_rounds here; a barrier must not. The sibling is a fresh
    # `.running` on one setup key that nothing will ever release, so the wait is ended by the
    # deadline, which is what proves it waited rather than returned.
    with_both() do main, prep, outdir
        pkeys = DataVault.keys(prep)
        DataVault.mark_running!(prep, pkeys[1])        # a sibling holds it
        built = Ref(0)

        t0 = time()
        pre = SweepRunner.run_prerequisite!(
            Prerequisite(
                k -> (built[] += 1; Dict{String,Any}("s" => 1)),
                prep,
                pkeys;
                opts=RunOpts(workers=:sequential, stale_after=600.0, deadline=time() + 1.5),
            );
            poll=0.2,
        )
        elapsed = time() - t0

        @test !pre.complete
        @test pre.stopped_by === :deadline
        @test pre.waited >= 1                  # it slept instead of giving up
        @test elapsed >= 1.0
        @test built[] == length(pkeys) - 1     # the other setup key was still built
        @test pre.remaining == 1
    end
end

@testset "prerequisite: opts on the Prerequisite override the dependent stage's" begin
    with_both() do main, prep, outdir
        p = Prerequisite(
            k -> Dict{String,Any}("s" => 1),
            prep,
            DataVault.keys(prep);
            opts=RunOpts(workers=:sequential, stale_after=1234.0),
        )
        @test p.opts.stale_after == 1234.0
        @test Prerequisite(k -> Dict{String,Any}(), prep, DataVault.keys(prep)).opts ===
            nothing

        pre = SweepRunner.run_prerequisite!(p; opts=RunOpts(workers=:sequential), poll=0.0)
        @test pre.complete
        @test pre.done == length(DataVault.keys(prep))
    end
end

@testset "prerequisite: an already-complete setup is a no-op, and resume works" begin
    with_both() do main, prep, outdir
        pkeys = DataVault.keys(prep)
        n = Ref(0)
        p() = Prerequisite(
            k -> (n[] += 1; Dict{String,Any}("s" => 1)),
            prep,
            pkeys;
            opts=RunOpts(workers=:sequential),
        )
        first = SweepRunner.run_prerequisite!(p(); poll=0.0)
        @test first.complete && first.done == length(pkeys)
        @test n[] == length(pkeys)

        second = SweepRunner.run_prerequisite!(p(); poll=0.0)
        @test second.complete
        @test second.done == 0                 # nothing rebuilt
        @test n[] == length(pkeys)
    end
end

@testset "prerequisite: run_loop! without one is unchanged, and now reports" begin
    with_both() do main, prep, outdir
        r = run_loop!(
            k -> Dict{String,Any}("x" => 1),
            main,
            DataVault.keys(main);
            opts=RunOpts(workers=:sequential),
            idle_sleep=0.0,
        )
        @test r.ran
        @test r.prerequisite === nothing
        @test r.done == length(DataVault.keys(main))
        @test r.stopped_by === nothing
    end
end

@testset "prerequisite: a stage may not loosen a bound the job set" begin
    # A Prerequisite's own `opts` must not let the barrier outlive the allocation running it,
    # whether by dropping the caller's deadline or by naming a more generous one of its own.
    with_both() do main, prep, outdir
        built = Ref(0)
        work = k -> (built[] += 1; Dict{String,Any}("s" => 1))
        keys = ParamIO.expand(prep.spec)

        p = Prerequisite(work, prep, keys; opts=RunOpts(stale_after=3600.0))
        r = run_prerequisite!(p; opts=RunOpts(deadline=time() - 1), poll=0.01)
        @test r.complete == false
        @test r.stopped_by === :deadline          # inherited from the caller
        @test built[] == 0                        # and no setup was handed out
        @test p.opts.stale_after == 3600.0        # while the field it WAS given still applies

        # The TIGHTER one governs, not the explicit one: a Prerequisite built with a generous
        # ceiling must not outlive a caller whose allocation is nearly over.
        p2 = Prerequisite(work, prep, keys; opts=RunOpts(deadline=time() + 3600))
        r2 = run_prerequisite!(p2; opts=RunOpts(deadline=time() - 1), poll=0.01)
        @test r2.complete == false
        @test r2.stopped_by === :deadline
        @test built[] == 0

        # Control the other way round: with no bound from the caller, the prerequisite's applies
        # and the work runs, so `false` above is the tighter bound and not a blanket refusal.
        r3 = run_prerequisite!(p2; opts=RunOpts(), poll=0.01)
        @test r3.complete == true
        @test built[] == length(keys)
    end
end

@testset "prerequisite: the caller's stop flag is the one that governs" begin
    # `RunOpts` resolves `stop_flag` from ENV["SWEEPRUNNER_STOP_FLAG"], so a Prerequisite built for
    # `stale_after` alone can still carry a flag it never asked for. Treating "non-nothing" as
    # "explicitly chosen" then let that ambient value outrank the flag the campaign was configured
    # with, and an operator raising the one they know about would never be seen by the barrier.
    with_both() do main, prep, outdir
        built = Ref(0)
        work = k -> (built[] += 1; Dict{String,Any}("s" => 1))
        keys = ParamIO.expand(prep.spec)

        ambient = joinpath(outdir, "AMBIENT_FLAG")
        callers = joinpath(outdir, "CALLERS_FLAG")
        touch(callers)                                # only the caller's flag is raised

        p = withenv("SWEEPRUNNER_STOP_FLAG" => ambient) do
            Prerequisite(work, prep, keys; opts=RunOpts(stale_after=3600.0))
        end
        @test p.opts.stop_flag == ambient             # carried without ever being asked for

        r = run_prerequisite!(p; opts=RunOpts(stop_flag=callers), poll=0.01)
        @test r.complete == false
        @test r.stopped_by === :flag                  # the caller's flag was seen
        @test built[] == 0

        # Control: with the caller's flag cleared the barrier runs, so `:flag` above is that file
        # and not a refusal for some other reason.
        rm(callers; force=true)
        r2 = run_prerequisite!(p; opts=RunOpts(stop_flag=callers), poll=0.01)
        @test r2.complete == true
        @test built[] == length(keys)
    end
end

@testset "prerequisite: an already-satisfied barrier is complete, stop pending or not" begin
    # `complete` was a hardcoded `false` the moment a stop was seen, computed on the same line as
    # `remaining`. A setup built by an earlier run therefore reported `complete=false, remaining=0`,
    # and `run_loop!` gates the dependent stage on exactly that field.
    with_both() do main, prep, outdir
        built = Ref(0)
        work = k -> (built[] += 1; Dict{String,Any}("s" => 1))
        keys = ParamIO.expand(prep.spec)

        p = Prerequisite(work, prep, keys)
        @test run_prerequisite!(p; opts=RunOpts(), poll=0.01).complete == true
        @test built[] == length(keys)

        r = run_prerequisite!(p; opts=RunOpts(deadline=time() - 1), poll=0.01)
        @test r.remaining == 0
        @test r.complete == true                      # and the two agree
        @test built[] == length(keys)                 # nothing was rebuilt
    end
end

@testset "prerequisite: a round cut short is not reported as a genuine dead end" begin
    # The no-progress exit propagates the round's own reason. Without it, a deadline that cut the
    # round short read as "nobody holds these and they will not appear", which is the answer that
    # tells a resubmitter not to bother.
    with_both() do main, prep, outdir
        keys = ParamIO.expand(prep.spec)
        @test length(keys) > 1
        work = k -> (sleep(3.0); error("cannot build this setup"))
        p = Prerequisite(work, prep, keys)
        r = run_prerequisite!(
            p;
            opts=RunOpts(deadline=time() + 2.0, max_attempts=1, workers=:sequential),
            poll=0.01,
        )
        @test r.complete == false
        @test r.remaining > 0
        @test r.stopped_by === :deadline              # cut short, not a dead end
    end
end

@testset "prerequisite: the documented project(...) recipe reproduces the setup key space" begin
    # `Prerequisite`'s docstring says to build `keys` by projecting the dependent key space onto
    # the axes the setup depends on, "so the two spaces cannot drift apart by hand". Every other
    # test in this file passes `DataVault.keys(prep)`, the hand-written projection that IS
    # prep.toml, so the recipe the docs recommend was never once executed.
    with_both() do main, prep, outdir
        projected = ParamIO.expand(ParamIO.project(main.spec, ["N"]; total_samples=1))
        @test Set(projected) == Set(DataVault.keys(prep))   # the hand-written file is the oracle

        builds = joinpath(outdir, "builds.txt")
        prep_fn = k -> (_log_build!(builds, "N$(setup_of(k))"); Dict{String,Any}("s" => 1))
        r = run_loop!(
            k -> Dict{String,Any}("x" => 1),
            main,
            DataVault.keys(main);
            prerequisite=Prerequisite(prep_fn, prep, projected),
            opts=RunOpts(workers=:sequential),
            idle_sleep=0.0,
        )
        @test r.ran
        @test r.prerequisite.complete
        @test _n_builds(builds, "N4") == 1                  # once per distinct N ...
        @test _n_builds(builds, "N8") == 1
        @test length(DataVault.keys(main)) == 4             # ... not once per dependent key

        # Control: the projection tracks a sweep that grows and the hand-written file does not,
        # which is the drift the recipe exists to remove. Without it the equality above would also
        # hold for a projection that ignored the spec entirely.
        grown = joinpath(outdir, "grown.toml")
        write(grown, replace(read(_PRE_MAIN, String), "N = [4, 8]" => "N = [4, 8, 12]"))
        gproj = ParamIO.expand(ParamIO.project(ParamIO.load(grown), ["N"]; total_samples=1))
        @test length(gproj) == 3
        @test Set(gproj) != Set(DataVault.keys(prep))
    end
end
